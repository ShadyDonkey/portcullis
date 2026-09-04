package gateway

type Intent int

const (
	IntentGuilds Intent = 1 << iota
	IntentGuildMembers
	IntentGuildModeration
	IntentGuildExpressions

	// TODO: the rest
)
