package gateway

type Opcode int

const (
	OpDispatch Opcode = iota
	OpHeartbeat
	OpIdentify
	OpPresenceUpdate
	OpVoiceStateUpdate
	_
	OpResume
	OpReconnect
	OpRequestGuildMembers
	OpInvalidSession
	OpHello
	OpHeartbeatAck
	OpRequestSoundboardSounds Opcode = 31
	OpRequestChannelInfo      Opcode = 43
)
