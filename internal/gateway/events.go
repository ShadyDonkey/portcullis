package gateway

type RecvHelloData struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

type SendIdentifyData struct {
	Token          string                 `json:"token"`
	Properties     SendIdentifyProperties `json:"properties"`
	Compress       *bool                  `json:"compress,omitempty"`
	LargeThreshold *int                   `json:"large_threshold,omitempty"`
	Shard          *[2]int                `json:"shard,omitempty"`
	Intents        int                    `json:"intents"`
}

type SendIdentifyProperties struct {
	OS      string `json:"os"`
	Browser string `json:"browser"`
	Device  string `json:"device"`
}
