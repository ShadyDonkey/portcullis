package config

import (
	"log/slog"
)

const (
	defaultConfigPath = "config.toml"
)

type Config struct {
	Discord       DiscordConfig       `toml:"discord"`
	Portcullis    PortcullisConfig    `toml:"portcullis"`
	NATS          NATSConfig          `toml:"nats"`
	HTTP          HTTPConfig          `toml:"http"`
	Observability ObservabilityConfig `toml:"observability"`
	Log           LogConfig           `toml:"log"`
}

type DiscordConfig struct {
	Token    EnvSecret      `toml:"token"`
	Intents  []string       `toml:"intents"`
	Sharding ShardingConfig `toml:"sharding"`
}

type ShardingConfig struct {
	ReshardCheckInterval *Duration `toml:"reshard_check_interval"`
	ShardCountOverride   *int      `toml:"shard_count_override"`
}

type PortcullisConfig struct {
	Publish []string `toml:"publish"`
}

type NATSConfig struct {
	URL            EnvVar           `toml:"url"`
	Username       EnvVar           `toml:"username"`
	Password       EnvSecret        `toml:"password"`
	ConnectionName string           `toml:"connection_name"`
	KV             NATSKVConfig     `toml:"kv"`
	Stream         NATSStreamConfig `toml:"stream"`
}

type NATSKVConfig struct {
	StateBucket string `toml:"state_bucket"`
}

type NATSStreamConfig struct {
	Name          string   `toml:"name"`
	SubjectPrefix string   `toml:"subject_prefix"`
	MaxAge        Duration `toml:"max_age"`
	MaxBytes      ByteSize `toml:"max_bytes"`
}

type HTTPConfig struct {
	Enabled bool           `toml:"enabled"`
	Listen  string         `toml:"listen"`
	Auth    HTTPAuthConfig `toml:"auth"`
	Healthz HealthzConfig  `toml:"healthz"`
}

type HTTPAuthConfig struct {
	Header string    `toml:"header"`
	Key    EnvSecret `toml:"key"`
}

type HealthzConfig struct {
	RequireAuth bool `toml:"require_auth"`
	ShowStats   bool `toml:"show_stats"`
}

type ObservabilityConfig struct {
	OtelEnabled   bool `toml:"otel_enabled"`
	SentryEnabled bool `toml:"sentry_enabled"`
}

type LogConfig struct {
	Level  slog.Level `toml:"level"`
	Format string     `toml:"format"`
}
