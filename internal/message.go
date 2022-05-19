package internal

import "time"

// WIS is the message as defined in https://github.com/wmo-im/GTStoWIS2/tree/main/message_format
type Message struct {
	Topic    string
	Received time.Time
	Source   string

	WIS struct {
		PubTime   *time.Time `json:"pubTime"`
		BaseURL   string     `json:"baseUrl"`
		RelPath   string     `json:"relPath"`
		Integrity struct {
			Method string `json:"method"`
			Value  string `json:"value"`
		}
		Size    int64  `json:"size"`
		RetPath string `json:"retPath"`
		Content struct {
			Encoding string `json:"encoding"`
			Value    []byte `json:"value"`
		}
		PartitionStrategy struct {
			Method      string `json:"partitioned"`
			BlockNumber int    `json:"blockNumber"`
			BlockCount  int    `json:"blockCount"`
			BlockSize   int    `json:"blockSize"`
			LastBlock   int    `json:"lastBlock"`
		}
	}
}
