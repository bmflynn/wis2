package internal

type Receiver interface {
	Err() error
	Next() bool
	Message() *Message
}
