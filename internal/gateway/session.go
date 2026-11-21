package gateway

type Session struct {
	ID               string `json:"session_id"`
	ResumeGatewayURL string `json:"resume_gateway_url"`
	LastSequence     *int   `json:"last_sequence"`
}

type SessionStore interface {
	GetSession(shardID int) (*Session, error)
	PutSession(shardID int, session Session) error
	DeleteSession(shardID int) error
}
