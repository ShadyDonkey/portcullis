package bridge

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

type Config interface {
	IsEnabled() bool
	Validate() error
}

type Factory struct {
	Name      string
	NewConfig func() Config
	New       func(ctx context.Context, cfg Config, env Env) (Bridge, error)
}

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

func Register(f Factory) {
	if !validName(f.Name) {
		// TODO: remove panic?
		panic(fmt.Sprintf("bridge: invalid name %q", f.Name))
	}

	if f.NewConfig == nil || f.New == nil {
		panic(fmt.Sprintf("bridge: factory %s needs both New and NewConfig", f.Name))
	}

	mu.Lock()
	defer mu.Unlock()

	if _, dupe := factories[f.Name]; dupe {
		panic(fmt.Sprintf("bridge: factory %s already registered", f.Name))
	}

	factories[f.Name] = f
}

func Lookup(name string) (Factory, bool) {
	mu.RLock()
	defer mu.RUnlock()

	f, ok := factories[name]

	return f, ok
}

func Names() []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0, len(factories))

	for name := range factories {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

func UnknownError(name string) error {
	return fmt.Errorf("bridges.%s: unknown bridge. Registered bridges: %s", name, strings.Join(Names(), ", "))
}

func validName(s string) bool {
	if s == "" {
		return false
	}

	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}

	return true
}
