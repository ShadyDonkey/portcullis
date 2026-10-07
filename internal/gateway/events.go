package gateway

type RecvHelloData struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

type RecvReadyData struct {
	SessionID        string  `json:"session_id"`
	ResumeGatewayURL string  `json:"resume_gateway_url"`
	Shard            *[2]int `json:"shard"`
}

type sendIdentifyData struct {
	Token          string                 `json:"token"`
	Properties     sendIdentifyProperties `json:"properties"`
	Compress       *bool                  `json:"compress,omitempty"`
	LargeThreshold *int                   `json:"large_threshold,omitempty"`
	Shard          *[2]int                `json:"shard,omitempty"`
	Intents        int                    `json:"intents"`
}

type sendResumeData struct {
	Token     string `json:"token"`
	SessionID string `json:"session_id"`
	Sequence  int    `json:"seq"`
}

type sendIdentifyProperties struct {
	OS      string `json:"os"`
	Browser string `json:"browser"`
	Device  string `json:"device"`
}
