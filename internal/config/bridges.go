package config

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/ShadyDonkey/portcullis/internal/bridge"
	"github.com/pelletier/go-toml/v2"
)

type BridgeEntry struct {
	Name    string
	Factory bridge.Factory
	Config  bridge.Config
}

func decodeBridges(raw map[string]map[string]any) ([]BridgeEntry, error) {
	names := make([]string, 0, len(raw))
	for name := range raw {
		names = append(names, name)
	}

	sort.Strings(names)

	var entries []BridgeEntry
	var errs []error

	for _, name := range names {
		factory, ok := bridge.Lookup(name)

		if !ok {
			errs = append(errs, bridge.UnknownError(name))
			continue
		}

		cfg, err := decodeBridge(name, factory, raw[name])
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if !cfg.IsEnabled() {
			continue
		}

		if err := cfg.Validate(); err != nil {
			errs = append(errs, prefixErrors(name, err))
			continue
		}

		entries = append(
			entries, BridgeEntry{
				Name:    name,
				Factory: factory,
				Config:  cfg,
			},
		)
	}

	return entries, errors.Join(errs...)
}

func decodeBridge(name string, factory bridge.Factory, table map[string]any) (bridge.Config, error) {
	data, err := toml.Marshal(table)
	if err != nil {
		return nil, fmt.Errorf("bridges.%s: %w", name, err)
	}

	cfg := factory.NewConfig()
	dec := toml.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	if err := dec.Decode(cfg); err != nil {
		var strict *toml.StrictMissingError

		if errors.As(err, &strict) {
			keys := make([]string, 0, len(strict.Errors))
			for _, e := range strict.Errors {
				keys = append(keys, strings.Join(e.Key(), "."))
			}

			return nil, fmt.Errorf("bridges.%s: unknown key(s): %s", name, strings.Join(keys, ", "))
		}

		return nil, fmt.Errorf("bridges.%s: %w", name, err)
	}

	return cfg, nil
}

func prefixErrors(name string, err error) error {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		out := make([]error, 0, len(joined.Unwrap()))

		for _, e := range joined.Unwrap() {
			out = append(out, fmt.Errorf("bridges.%s: %w", name, e))
		}

		return errors.Join(out...)
	}

	return fmt.Errorf("bridges.%s: %w", name, err)
}
