package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/bridge"
	"github.com/ShadyDonkey/portcullis/internal/metrics"
)

const (
	dialTimeout    = 3 * time.Second
	maxRetryAfter  = 30 * time.Second
	maxBackoff     = 5 * time.Second
	bodySnippet    = 512
	drainLimit     = 64 << 10
	requestTimeout = 5 * time.Second
	backoffBase    = 250 * time.Millisecond
)

func init() {
	bridge.Register(
		bridge.Factory{
			Name:      "webhook",
			NewConfig: func() bridge.Config { return defaultConfig() },
			New:       newBridge,
		},
	)
}

type Bridge struct {
	name          string
	client        *http.Client
	target        string
	query         string
	eventInPath   bool
	fireAndForget bool
	maxRetries    int
	authHeader    string
	authKey       string
	fail          *bridge.FailureLog
	ctx           context.Context
	cancel        context.CancelFunc
	stopOnce      sync.Once
}

func newBridge(_ context.Context, c bridge.Config, env bridge.Env) (bridge.Bridge, error) {
	cfg, ok := c.(*Config)
	if !ok {
		return nil, fmt.Errorf("webhook: unexpected config type %T", c)
	}

	u, err := url.Parse(cfg.URL.String())
	if err != nil {
		return nil, fmt.Errorf("webhook: %w", err)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext
	transport.MaxIdleConnsPerHost = 2

	ctx, cancel := context.WithCancel(context.Background())

	b := &Bridge{
		name:          env.Name,
		client:        &http.Client{Transport: transport, Timeout: requestTimeout},
		eventInPath:   cfg.EventInPath,
		fireAndForget: cfg.FireAndForget,
		maxRetries:    cfg.MaxRetries,
		fail:          bridge.NewFailureLog(env.Logger),
		ctx:           ctx,
		cancel:        cancel,
	}

	if !cfg.Auth.Key.IsEmpty() {
		b.authHeader = cfg.Auth.Header
		b.authKey = cfg.Auth.Key.Reveal()
	}

	if cfg.EventInPath {
		if u.RawQuery != "" {
			b.query = "?" + u.RawQuery
		}

		u.RawQuery = ""
		u.Fragment = ""
		b.target = strings.TrimRight(u.String(), "/")
	} else {
		b.target = u.String()
	}

	return b, nil
}

func (b *Bridge) Deliver(e *bridge.Event) error {
	defer e.Release()

	if b.fireAndForget {
		b.fire(e)
		return nil
	}

	b.deliver(e)

	return nil
}

func (b *Bridge) Close(context.Context) error {
	b.stopOnce.Do(
		func() {
			b.cancel()
			b.client.CloseIdleConnections()
		},
	)

	return nil
}

func (b *Bridge) urlFor(e *bridge.Event) string {
	if !b.eventInPath {
		return b.target
	}

	return b.target + "/" + url.PathEscape(e.Type) + b.query
}

func (b *Bridge) newRequest(e *bridge.Event) (*http.Request, error) {
	req, err := http.NewRequestWithContext(b.ctx, http.MethodPost, b.urlFor(e), bytes.NewReader(e.Frame))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "portcullis")
	req.Header.Set("Discord-Event", e.Type)
	req.Header.Set("Discord-Shard", strconv.Itoa(e.Shard))
	req.Header.Set("Discord-Sequence", strconv.Itoa(e.Sequence))

	if b.authHeader != "" {
		req.Header.Set(b.authHeader, b.authKey)
	}

	return req, nil
}

func (b *Bridge) fire(e *bridge.Event) {
	req, err := b.newRequest(e)
	if err != nil {
		return
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return
	}

	metrics.Payload(b.name, len(e.Frame))
	metrics.Published(b.name, e.Type)

	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit))
	_ = resp.Body.Close()
}

func (b *Bridge) deliver(e *bridge.Event) {
	start := time.Now()

	for attempt := 0; ; attempt++ {
		req, err := b.newRequest(e)
		if err != nil {
			b.drop(metrics.DropSyncError, "bad_request", err, e)
			return
		}

		resp, err := b.client.Do(req)
		if err != nil {
			if !isConnectFailure(err) {
				b.drop(metrics.DropSyncError, "request_failed", err, e)
				return
			}

			if !b.retryAfter(attempt, 0) {
				b.drop(metrics.DropSyncError, "cannot_connect", err, e)
				return
			}

			continue
		}

		status := resp.StatusCode
		snippet := readSnippet(resp)
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))

		switch {
		case status >= 200 && status < 300:
			metrics.Ack(b.name, e.Type, time.Since(start))
			metrics.Payload(b.name, len(e.Frame))
			metrics.Published(b.name, e.Type)
			return

		case status == http.StatusTooManyRequests:
			if !b.retryAfter(attempt, retryAfter) {
				b.drop(
					metrics.DropAckError, "retries_exhausted",
					fmt.Errorf("receiver kept answering 429 after %d retries", b.maxRetries), e,
				)
				return
			}

		default:
			b.drop(
				metrics.DropAckError, "bad_status",
				fmt.Errorf("receiver answered %d: %s", status, snippet), e,
			)
			return
		}
	}
}

func (b *Bridge) retryAfter(attempt int, wait time.Duration) bool {
	if attempt >= b.maxRetries {
		return false
	}

	if wait <= 0 {
		wait = min(backoffBase<<attempt, maxBackoff)
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-b.ctx.Done():
		return false
	}
}

func (b *Bridge) drop(reason, kind string, err error, e *bridge.Event) {
	metrics.BridgeDrop(b.name, reason)
	b.fail.Record(kind, err, "event", e.Type, "shard", e.Shard, "seq", e.Sequence)
}

func readSnippet(resp *http.Response) string {
	defer resp.Body.Close()

	buf, _ := io.ReadAll(io.LimitReader(resp.Body, bodySnippet))
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit))

	return strings.TrimSpace(string(buf))
}

func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}

	var d time.Duration

	if secs, err := strconv.Atoi(v); err == nil {
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(v); err == nil {
		d = time.Until(t)
	}

	return max(0, min(d, maxRetryAfter))
}

func isConnectFailure(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}

	var dnsErr *net.DNSError

	return errors.As(err, &dnsErr)
}
