package internal

import (
	"context"
)

type RecvMessage struct {
	Message *Message
	Err     error
}

type Receiver func(context.Context) chan RecvMessage
