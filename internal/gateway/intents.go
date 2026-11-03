package gateway

import (
	"fmt"
	"strings"
)

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

var intentNames = map[string]Intent{
	"GUILDS":                        IntentGuilds,
	"GUILD_MEMBERS":                 IntentGuildMembers,
	"GUILD_MODERATION":              IntentGuildModeration,
	"GUILD_EXPRESSIONS":             IntentGuildExpressions,
	"GUILD_INTEGRATIONS":            IntentGuildIntegrations,
	"GUILD_WEBHOOKS":                IntentGuildWebhooks,
	"GUILD_INVITES":                 IntentGuildInvites,
	"GUILD_VOICE_STATES":            IntentGuildVoiceStates,
	"GUILD_PRESENCES":               IntentGuildPresences,
	"GUILD_MESSAGES":                IntentGuildMessages,
	"GUILD_MESSAGE_REACTIONS":       IntentGuildMessageReactions,
	"GUILD_MESSAGE_TYPING":          IntentGuildMessageTyping,
	"DIRECT_MESSAGES":               IntentDirectMessages,
	"DIRECT_MESSAGE_REACTIONS":      IntentDirectMessageReactions,
	"DIRECT_MESSAGE_TYPING":         IntentDirectMessageTyping,
	"MESSAGE_CONTENT":               IntentMessageContent,
	"GUILD_SCHEDULED_EVENTS":        IntentGuildScheduledEvents,
	"AUTO_MODERATION_CONFIGURATION": IntentAutoModerationConfiguration,
	"AUTO_MODERATION_EXECUTION":     IntentAutoModerationExecution,
	"GUILD_MESSAGE_POLLS":           IntentGuildMessagePolls,
	"DIRECT_MESSAGE_POLLS":          IntentDirectMessagePolls,
}

func ParseIntents(names []string) (Intent, error) {
	var intents Intent
	var unknown []string

	for _, name := range names {
		bit, ok := intentNames[name]

		if !ok {
			unknown = append(unknown, name)
			continue
		}

		intents |= bit
	}

	if len(unknown) > 0 {
		return 0, fmt.Errorf("unknown Intents: %v", strings.Join(unknown, ", "))
	}

	return intents, nil
}
