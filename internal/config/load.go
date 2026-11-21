package config

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/pelletier/go-toml/v2"
)

const defaultDataPath = "data"

var defaultConfigPath = fmt.Sprintf("%s/config.toml", defaultDataPath)

func defaults() *Config {
	return &Config{
		Portcullis: PortcullisConfig{
			StatePath: fmt.Sprintf("%s/state.db", defaultDataPath),
		},
		HTTP: HTTPConfig{
			Listen: ":3333",
			Healthz: HealthzConfig{
				RequireAuth: true,
				ShowStats:   false,
			},
			Auth: HTTPAuthConfig{
				Header: "X-API-Key",
			},
		},
		Log: LogConfig{
			Level:  slog.LevelInfo,
			Format: "json",
		},
	}
}

func Load() (*Config, error) {
	data, err := os.ReadFile(defaultConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := defaults()
	decoded := toml.NewDecoder(bytes.NewReader(data))
	decoded.DisallowUnknownFields()

	decodeErr := decoded.Decode(cfg)
	if decodeErr != nil {
		if strict, ok := errors.AsType[*toml.StrictMissingError](decodeErr); ok {
			return nil, fmt.Errorf("decode %s:\n%s", defaultConfigPath, strict.String())
		}

		if decode, ok := errors.AsType[*toml.DecodeError](decodeErr); ok {
			return nil, fmt.Errorf("decode %s:\n%s", defaultConfigPath, decode.String())
		}

		return nil, decodeErr
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}
