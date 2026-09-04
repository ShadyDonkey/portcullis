package gateway

type Intent int

const (
	IntentGuilds Intent = 1 << iota
	IntentGuildMembers
	IntentGuildModeration
	IntentGuildExpressions
	IntentGuildIntegrations
	IntentGuildWebhooks
	IntentGuildInvites
	IntentGuildVoiceStates
	IntentGuildPresences
	IntentGuildMessages
	IntentGuildMessageReactions
	IntentGuildMessageTyping
	IntentDirectMessages
	IntentDirectMessageReactions
	IntentDirectMessageTyping
	IntentMessageContent
	IntentGuildScheduledEvents
	_
	_
	_
	IntentAutoModerationConfiguration
	IntentAutoModerationExecution
	_
	_
	IntentGuildMessagePolls
	IntentDirectMessagePolls
)
