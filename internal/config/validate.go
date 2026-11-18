package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
)

var (
	namePattern   = regexp.MustCompile("^[a-zA-Z0-9_]+$")
	headerPattern = regexp.MustCompile("^[a-zA-Z0-9_-]+$")
)

func (c *Config) Validate() error {
	var errs []error

	add := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	required := func(key string, missing []string, empty bool) {
		switch {
		case len(missing) > 0:
			refs := make([]string, len(missing))
			for i, name := range missing {
				refs[i] = fmt.Sprintf("${%s}", name)
			}

			add("%s: %s is not set", key, strings.Join(refs, ", "))
		case empty:
			add("%s is required and must not be empty", key)
		}
	}

	required("discord.token", c.Discord.Token.Missing(), c.Discord.Token.IsEmpty())
	if len(c.Discord.Intents) == 0 {
		add("discord.intents: at least one intent is required")
	}

	ds := c.Discord.Sharding
	dsHasInterval := ds.ReshardCheckInterval != nil
	dsHasOverride := ds.ShardCountOverride != nil

	if !dsHasInterval && !dsHasOverride {
		add("discord.sharding: either reshard_check_interval or shard_count_override is required")
	}

	if dsHasInterval && dsHasOverride {
		add("discord.sharding: reshard_check_interval and shard_count_override cannot both be set")
	}

	if dsHasInterval && *ds.ReshardCheckInterval <= 0 {
		add("discord.sharding.reshard_check_interval must be greater than zero")
	}

	if dsHasOverride && *ds.ShardCountOverride <= 0 {
		add("discord.sharding.shard_count_override must be greater than zero")
	}

	if len(c.Portcullis.Publish) == 0 {
		add("portcullis.publish: list must have at least one event name or [\"*\"] for all")
	}

	for _, name := range c.Portcullis.Publish {
		if name == "" {
			add("portcullis.publish: event names must not be empty")
		}

		if name == "*" && len(c.Portcullis.Publish) > 1 {
			add("portcullis.publish: \"*\" must be the only entry when present")
		}
	}

	required("nats.url", c.NATS.URL.Missing(), c.NATS.URL.String() == "")
	required("nats.username", c.NATS.Username.Missing(), c.NATS.Username.String() == "")
	required("nats.password", c.NATS.Password.Missing(), c.NATS.Password.IsEmpty())

	if c.NATS.ConnectionName == "" {
		add("nats.connection_name must not be empty")
	}

	for _, n := range []struct{ k, v string }{
		{"nats.kv.state_bucket", c.NATS.KV.StateBucket},
		{"nats.stream.name", c.NATS.Stream.Name},
		{"nats.stream.subject_prefix", c.NATS.Stream.SubjectPrefix},
	} {
		if !namePattern.MatchString(n.v) {
			add("%s: %q must contain only numbers, letters, and underscores", n.k, n.v)
		}
	}

	if c.NATS.Stream.MaxAge <= 0 {
		add("nats.stream.max_age must be greater than zero")
	}

	if c.NATS.Stream.MaxBytes <= 0 {
		add("nats.stream.max_bytes must be greater than zero")
	}

	if c.HTTP.Enabled {
		if c.HTTP.Listen == "" {
			add("http.listen must not be empty")
		}

		if !headerPattern.MatchString(c.HTTP.Auth.Header) {
			add(
				"http.auth.header: %q must be set and contain only letters, numbers, dashes, and underscores",
				c.HTTP.Auth.Header,
			)
		}

		required("http.auth.key", c.HTTP.Auth.Key.Missing(), c.HTTP.Auth.Key.IsEmpty())
	}

	if c.Observability.SentryEnabled && os.Getenv("SENTRY_DSN") == "" {
		add("observability.sentry_enabled: Sentry enabled but no SENTRY_DSN environment variable set")
	}

	if !slices.Contains([]string{"json", "text"}, c.Log.Format) {
		add("log.format: %q must contain [\"json\", \"text\"]", c.Log.Format)
	}

	return errors.Join(errs...)
}
