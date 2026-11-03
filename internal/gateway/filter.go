package gateway

type EventFilter struct {
	all bool
	set map[string]struct{}
}

func NewEventFilter(names []string) EventFilter {
	f := EventFilter{
		set: make(map[string]struct{}, len(names)),
	}

	for _, name := range names {
		if name == "*" {
			f.all = true
			continue
		}

		f.set[name] = struct{}{}
	}

	return f
}

func (f EventFilter) Allow(eventType string) bool {
	if f.all {
		return true
	}

	_, ok := f.set[eventType]
	return ok
}
