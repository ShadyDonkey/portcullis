package jetstream

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/ShadyDonkey/portcullis/internal/bridge"
	"github.com/ShadyDonkey/portcullis/internal/cfgtypes"
)

var namePattern = regexp.MustCompile("^[a-zA-Z0-9_]+$")

type Config struct {
	bridge.Base

	URL            cfgtypes.EnvVar    `toml:"url"`
	Username       cfgtypes.EnvVar    `toml:"username"`
	Password       cfgtypes.EnvSecret `toml:"password"`
	ConnectionName string             `toml:"connection_name"`
	Stream         StreamConfig       `toml:"stream"`
}

type StreamConfig struct {
	Name          string            `toml:"name"`
	SubjectPrefix string            `toml:"subject_prefix"`
	MaxAge        cfgtypes.Duration `toml:"max_age"`
	MaxBytes      cfgtypes.ByteSize `toml:"max_bytes"`
}

func defaultConfig() *Config {
	return &Config{
		ConnectionName: "portcullis",
		Stream: StreamConfig{
			Name:          "DISCORD_EVENTS",
			SubjectPrefix: "events",
			MaxAge:        cfgtypes.Duration(2 * time.Hour),
			MaxBytes:      256_000_000,
		},
	}
}

func (c *Config) Validate() error {
	var errs []error

	if v := c.URL; len(v.Missing()) > 0 {
		errs = append(errs, fmt.Errorf("url: ${%s} is not set", v.Missing()[0]))
	} else if v.String() == "" {
		errs = append(errs, errors.New("url is required"))
	}

	if v := c.Username; len(v.Missing()) > 0 {
		errs = append(errs, fmt.Errorf("username: ${%s} is not set", v.Missing()[0]))
	} else if v.String() == "" {
		errs = append(errs, errors.New("username is required"))
	}

	if v := c.Password; len(v.Missing()) > 0 {
		errs = append(errs, fmt.Errorf("password: ${%s} is not set", v.Missing()[0]))
	} else if v.IsEmpty() {
		errs = append(errs, errors.New("password is required"))
	}

	for _, n := range []struct{ k, v string }{
		{"stream.name", c.Stream.Name},
		{"stream.subject_prefix", c.Stream.SubjectPrefix},
	} {
		if !namePattern.MatchString(n.v) {
			errs = append(errs, fmt.Errorf("%s: %q must contain only numbers, letters, and underscores", n.k, n.v))
		}
	}

	if c.Stream.MaxAge <= 0 {
		errs = append(errs, errors.New("stream.max_age must be greater than zero"))
	}

	if c.Stream.MaxBytes <= 0 {
		errs = append(errs, errors.New("stream.max_bytes must be greater than zero"))
	}

	return errors.Join(errs...)
}
