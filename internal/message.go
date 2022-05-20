package internal

import (
	"fmt"
	"net/url"
	"path"
	"regexp"
	"time"
)

var supportedIntegrities = regexp.MustCompile(`^(?:md5|sha256|sha512)$`)

type WISMessage struct {
	PubTime   *time.Time `json:"pubTime"`
	BaseURL   string     `json:"baseUrl"`
	RelPath   string     `json:"relPath"`
	Integrity struct {
		Method string `json:"method"`
		Value  string `json:"value"`
	}
	Size    int64  `json:"size"`
	RetPath string `json:"retPath"`
}

func (msg WISMessage) URL() string {
	relpath := msg.RelPath
	if relpath == "" {
		relpath = msg.RetPath
	}
	u, err := url.Parse(path.Join(msg.BaseURL, relpath))
	if err != nil {
		return ""
	}
	return u.String()
}

func (msg WISMessage) IsValid() error {
	if msg.URL() == "" {
		return fmt.Errorf("unable to construct URL")
	}
	if !supportedIntegrities.MatchString(msg.Integrity.Method) {
		return fmt.Errorf("unsupported integrity alg '%s'", msg.Integrity.Method)
	}
	switch {
	case msg.Integrity.Method == "sha512" && len(msg.Integrity.Value) != 128:
		return fmt.Errorf("invalid sha512: %s", msg.Integrity.Value)
	case msg.Integrity.Method == "sha256" && len(msg.Integrity.Value) != 64:
		return fmt.Errorf("invalid sha512: %s", msg.Integrity.Value)
	case msg.Integrity.Method == "md5" && len(msg.Integrity.Value) != 48:
		return fmt.Errorf("invalid %s: %s", msg.Integrity.Method, msg.Integrity.Value)
	}
	return nil
}

// WIS is the message as defined in https://github.com/wmo-im/GTStoWIS2/tree/main/message_format
type Message struct {
	Topic    string
	Received time.Time
	Source   string
	Payload  WISMessage
}
