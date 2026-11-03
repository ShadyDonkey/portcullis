package config

import (
	"encoding"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
)

const (
	redacted = "[redacted]"
)

type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	x, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}

	*d = Duration(x)
	return nil
}

func (d *Duration) String() string {
	return time.Duration(*d).String()
}

func mustParseDuration(s string) Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(fmt.Sprintf("failed to parse duration: %s", s))
	}

	return Duration(d)
}

type ByteSize int64

func (b *ByteSize) UnmarshalText(text []byte) error {
	n, err := humanize.ParseBytes(string(text))
	if err != nil {
		return err
	}

	*b = ByteSize(n)
	return nil
}

func (b *ByteSize) String() string {
	return humanize.Bytes(uint64(*b))
}

func mustParseByteSize(s string) ByteSize {
	n, err := humanize.ParseBytes(s)
	if err != nil {
		panic(fmt.Sprintf("failed to parse byte size: %s", s))
	}

	return ByteSize(n)
}

type EnvVar struct {
	value   string
	missing []string
}

func (e *EnvVar) UnmarshalText(text []byte) error {
	v, missing, err := expand(string(text))

	if err != nil {
		return err
	}

	e.value = v
	e.missing = missing

	return nil
}

func (e *EnvVar) String() string {
	return e.value
}

func (e *EnvVar) Missing() []string {
	return e.missing
}

type EnvSecret struct {
	value   string
	missing []string
}

func (e *EnvSecret) UnmarshalText(text []byte) error {
	v, missing, err := expand(string(text))

	if err != nil {
		return err
	}

	e.value = v
	e.missing = missing

	return nil
}

func (EnvSecret) String() string {
	return redacted
}

func (EnvSecret) Format(f fmt.State, _ rune) {
	_, _ = io.WriteString(f, redacted)
}

func (EnvSecret) MarshalText() ([]byte, error) {
	return []byte(redacted), nil
}

func (EnvSecret) LogValue() slog.Value {
	return slog.StringValue(redacted)
}

func (e EnvSecret) Reveal() string {
	return e.value
}

func (e *EnvSecret) Missing() []string {
	return e.missing
}

func (e EnvSecret) IsEmpty() bool {
	return e.value == ""
}

var (
	_ fmt.Stringer           = EnvSecret{}
	_ fmt.Formatter          = EnvSecret{}
	_ encoding.TextMarshaler = EnvSecret{}
	_ slog.LogValuer         = EnvSecret{}
)

func expand(s string) (out string, missing []string, err error) {
	var b strings.Builder

	for i := 0; i < len(s); i++ {
		c := s[i]

		if c != '$' || i+1 >= len(s) {
			b.WriteByte(s[i])
			continue
		}

		switch s[i+1] {
		case '$':
			b.WriteByte(s[i])
			i++
		case '{':
			end := strings.IndexByte(s[i+2:], '}')
			if end < 0 {
				return "", nil, fmt.Errorf("unterminated \"${\" at position %d", i)
			}

			name := s[i+2 : i+2+end]
			if !validEnvName(name) {
				return "", nil, fmt.Errorf("invalid env var name %q in ${...}", name)
			}

			if v, ok := os.LookupEnv(name); ok {
				b.WriteString(v)
			} else if !slices.Contains(missing, name) {
				missing = append(missing, name)
			}

			i += 2 + end
		default:
			b.WriteByte(c)
		}
	}

	return b.String(), missing, nil
}

func validEnvName(name string) bool {
	if len(name) < 1 || name == "" || name == "." || name == ".." || name == "_" || name == "-" || name == "~" {
		return false
	}

	for i := 0; i < len(name); i++ {
		c := name[i]

		letter := c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
		digit := c >= '0' && c <= '9'

		if !letter && !(digit && i > 0) {
			return false
		}
	}

	return true
}
