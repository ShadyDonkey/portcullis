package webhook

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"

	"github.com/ShadyDonkey/portcullis/internal/bridge"
	"github.com/ShadyDonkey/portcullis/internal/cfgtypes"
)

var headerPattern = regexp.MustCompile("^[a-zA-Z0-9_-]+$")

const maxMaxRetries = 10

type Config struct {
	bridge.Base

	URL           cfgtypes.EnvVar `toml:"url"`
	EventInPath   bool            `toml:"event_in_path"`
	FireAndForget bool            `toml:"fire_and_forget"`
	MaxRetries    int             `toml:"max_retries"`
	Auth          AuthConfig      `toml:"auth"`
}

type AuthConfig struct {
	Key    cfgtypes.EnvSecret `toml:"key"`
	Header string             `toml:"header"`
}

func defaultConfig() *Config {
	return &Config{
		MaxRetries: 3,
		Auth:       AuthConfig{Header: "X-API-Key"},
	}
}

func (c *Config) Validate() error {
	var errs []error

	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	switch {
	case len(c.URL.Missing()) > 0:
		add("url: ${%s} is not set", c.URL.Missing()[0])
	case c.URL.String() == "":
		add("url is required")
	default:
		u, err := url.Parse(c.URL.String())

		switch {
		case err != nil:
			add("url: %v", err)
		case u.Scheme != "http" && u.Scheme != "https":
			add("url: scheme must be http or https")
		case u.Host == "":
			add("url: host must not be empty")
		}
	}

	if c.MaxRetries < 0 || c.MaxRetries > maxMaxRetries {
		add("max_retries must be between 0 and %d", maxMaxRetries)
	}

	if len(c.Auth.Key.Missing()) > 0 {
		add("auth.key: ${%s} is not set", c.Auth.Key.Missing()[0])
	}

	if !c.Auth.Key.IsEmpty() && !headerPattern.MatchString(c.Auth.Header) {
		add("auth.header: %q must contain only letters, numbers, dashes, and underscores", c.Auth.Header)
	}

	return errors.Join(errs...)
}
