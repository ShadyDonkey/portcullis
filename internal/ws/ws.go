package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/framepool"
	"github.com/coder/websocket"
)

const writeTimeout = 5 * time.Second

type Conn struct {
	conn           *websocket.Conn
	maxMessageSize int64
}

func Dial(ctx context.Context, url string, maxMessageSize int64) (*Conn, error) {
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}

	conn.SetReadLimit(maxMessageSize)
	return &Conn{conn: conn, maxMessageSize: maxMessageSize}, nil
}

func (c *Conn) ReadFrame(ctx context.Context) ([]byte, error) {
	_, r, err := c.conn.Reader(ctx)
	if err != nil {
		return nil, err
	}

	buf := framepool.Get(0)

	var readErr error
	for readErr == nil {
		if len(buf) == cap(buf) {
			if cap(buf) >= int(c.maxMessageSize) {
				framepool.Put(buf)
				return nil, fmt.Errorf("websocket frame exceeds %d bytes", c.maxMessageSize)
			}

			bigger := framepool.Get(cap(buf) * 2)
			bigger = bigger[:cap(bigger)]
			copy(bigger, buf)
			bigger = bigger[:len(buf)]
			framepool.Put(buf)
			buf = bigger
		}

		var n int
		n, readErr = r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
	}

	if readErr != io.EOF {
		framepool.Put(buf)
		return nil, readErr
	}

	if cap(buf) > framepool.MaxPoolSize {
		return buf[:len(buf):len(buf)], nil
	}

	return buf, nil
}

func (c *Conn) WriteJSON(ctx context.Context, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	if err = c.conn.Write(writeCtx, websocket.MessageText, data); err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}

	return nil
}

func (c *Conn) Close() error {
	return c.conn.CloseNow()
}

func CloseStatus(err error) int {
	return int(websocket.CloseStatus(err))
}
