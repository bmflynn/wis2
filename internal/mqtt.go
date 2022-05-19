package internal

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/eclipse/paho.golang/paho"
)

type BrokerMessage struct {
	Message *Message
	Err     error
}

// ReceiveError is any error that occurs receiving or decoding a message
type ReceiveError struct {
	Topic string
	Body  []byte
	Err   error
}

func (e *ReceiveError) Error() string {
	return e.Err.Error()
}

type BrokerOptions struct {
	ClientID     string
	User, Passwd string
	KeepAlive    uint16
	CleanStart   bool
	Debug        bool
	QoS          int
}

func NewDefaultBrokerOptions() BrokerOptions {
	return BrokerOptions{
		ClientID:   "",
		User:       "",
		Passwd:     "",
		KeepAlive:  30,
		CleanStart: true,
		Debug:      false,
		QoS:        1,
	}
}

func (bo *BrokerOptions) SetCredentialsFromEnv(pfx string) {
	if v, ok := os.LookupEnv(fmt.Sprintf("%s_USER", pfx)); ok {
		bo.User = v
	}
	if v, ok := os.LookupEnv(fmt.Sprintf("%s_PASSWD", pfx)); ok {
		bo.Passwd = v
	}
}

var defaultDecoder = jsonDecoder

type Broker struct {
	log    *log.Logger
	opts   BrokerOptions
	client *paho.Client
	decode decoder

	// All publishings will be received on this channel
	publishings chan *paho.Publish
}

func NewBroker(url string, opts BrokerOptions) (*Broker, error) {
	conn, err := net.Dial("tcp", url)
	if err != nil {
		log.Fatalf("Failed to connect to %s: %s", url, err)
	}

	b := &Broker{
		log:    log.New(os.Stderr, "[broker]", log.LstdFlags),
		opts:   opts,
		decode: defaultDecoder,
	}
	c := paho.NewClient(paho.ClientConfig{
		ClientID: opts.ClientID,
		Conn:     conn,
		Router: paho.NewSingleHandlerRouter(func(m *paho.Publish) {
			b.publishings <- m
		}),
	})
	c.SetErrorLogger(b.log)
	if opts.Debug {
		b.client.SetDebugLogger(b.log)
	}
	b.client = c
	return b, nil
}

// Connect to the broker. Returns a boolean indicating if there is already a
// session present for the client and any error connecting. The session present
// flag is always false if an error is returned.
func (b *Broker) Connect(ctx context.Context) (bool, error) {
	req := &paho.Connect{
		KeepAlive:  b.opts.KeepAlive,
		ClientID:   b.opts.ClientID,
		CleanStart: b.opts.CleanStart,
		Username:   b.opts.User,
		Password:   []byte(b.opts.Passwd),
	}
	req.UsernameFlag = b.opts.User != ""
	req.PasswordFlag = b.opts.Passwd != ""

	resp, err := b.client.Connect(ctx, req)
	// Docs are indicate there may be a connack if there is an error
	if resp != nil && err != nil {
		return false, fmt.Errorf("[%v] %s: %w", resp.ReasonCode, resp.Properties.ReasonString, err)
	} else if err != nil {
		return false, err
	}
	if resp.ReasonCode != 0 {
		return false, fmt.Errorf("[%v] %s", resp.ReasonCode, resp.Properties.ReasonString)
	}

	go func() {
		<-ctx.Done()
		b.client.Disconnect(&paho.Disconnect{ReasonCode: 0})
	}()

	return resp.SessionPresent, nil
}

func (b *Broker) Subscribe(topics ...string) ([]string, error) {
	scrips := map[string]paho.SubscribeOptions{}
	for _, topic := range topics {
		scrips[topic] = paho.SubscribeOptions{QoS: byte(b.opts.QoS)}
	}
	sa, err := b.client.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: scrips,
	})
	if err != nil {
		return topics, err
	}
	failures := []string{}
	for idx, reason := range sa.Reasons {
		if reason != byte(b.opts.QoS) {
			failures = append(failures, topics[idx])
		}
	}
	return failures, nil
}

func (b *Broker) Receive(ctx context.Context) chan BrokerMessage {
	out := make(chan BrokerMessage)
	go func() {
		defer close(out)
		for {
			select {
			case pub, ok := <-b.publishings:
				if !ok {
					return // closed upstream
				}
				msg := BrokerMessage{}
				wisMsg, err := b.decode(pub.Payload)
				if err != nil {
					msg.Err = &ReceiveError{pub.Topic, pub.Payload, err}
				} else {
					msg.Message = &wisMsg
				}
				out <- msg
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
