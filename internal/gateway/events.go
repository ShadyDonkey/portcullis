package gateway

type RecvHelloData struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

type RecvReadyData struct {
	SessionID        string  `json:"session_id"`
	ResumeGatewayURL string  `json:"resume_gateway_url"`
	Shard            *[2]int `json:"shard"`
}

// TODO: do we need compress and large_threshold here?
type IdentifyData struct {
	Token      string             `json:"token"`
	Properties IdentifyProperties `json:"properties"`
	Shard      *[2]int            `json:"shard,omitempty"`
	Intents    int                `json:"intents"`
}

type ResumeData struct {
	Token     string `json:"token"`
	SessionID string `json:"session_id"`
	Sequence  int    `json:"seq"`
}

type IdentifyProperties struct {
	OS      string `json:"os"`
	Browser string `json:"browser"`
	Device  string `json:"device"`
}
