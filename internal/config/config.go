package config

import (
	"log/slog"

	"github.com/ShadyDonkey/portcullis/internal/cfgtypes"
)

type Config struct {
	Discord       DiscordConfig             `toml:"discord"`
	Portcullis    PortcullisConfig          `toml:"portcullis"`
	HTTP          HTTPConfig                `toml:"http"`
	Observability ObservabilityConfig       `toml:"observability"`
	Log           LogConfig                 `toml:"log"`
	RawBridges    map[string]map[string]any `toml:"bridges"`
	Bridges       []BridgeEntry             `toml:"-"`
}

type DiscordConfig struct {
	Token      cfgtypes.EnvSecret `toml:"token"`
	Intents    []string           `toml:"intents"`
	Sharding   ShardingConfig     `toml:"sharding"`
	APIBaseURL cfgtypes.EnvVar    `toml:"api_base_url"`
}

type ShardingConfig struct {
	ReshardCheckInterval *cfgtypes.Duration `toml:"reshard_check_interval"`
	ShardCountOverride   *int               `toml:"shard_count_override"`
}

type PortcullisConfig struct {
	Publish   []string `toml:"publish"`
	StatePath string   `toml:"state_path"`
}

type HTTPConfig struct {
	Enabled bool           `toml:"enabled"`
	Listen  string         `toml:"listen"`
	Auth    HTTPAuthConfig `toml:"auth"`
	Healthz HealthzConfig  `toml:"healthz"`
}

type HTTPAuthConfig struct {
	Header string             `toml:"header"`
	Key    cfgtypes.EnvSecret `toml:"key"`
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
