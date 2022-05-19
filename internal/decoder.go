package internal

import "encoding/json"

type decoder func([]byte) (Message, error)

func jsonDecoder(dat []byte) (Message, error) {
	var msg Message
	err := json.Unmarshal(dat, &msg)
	return msg, err
}
