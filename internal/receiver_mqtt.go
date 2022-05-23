package internal

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	_url "net/url"
	"os"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// ReceiveError is any error that occurs receiving or decoding a message
type ReceiveError struct {
	Topic string
	Body  []byte
	Err   error
}

func (e *ReceiveError) Error() string {
	return e.Err.Error()
}

type TopicsError struct {
	Failed []string
}

func (e *TopicsError) Error() string {
	return "failed subscriptions for " + strings.Join(e.Failed, ", ")
}

func connectTCP(uri string, tlsConfig *tls.Config) (net.Conn, error) {
	u, err := parseURL(uri)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "tcp":
		return net.Dial("tcp", u.Host)
	case "ssl":
		return tls.Dial("tcp", u.Host, tlsConfig)
	default:
		return nil, fmt.Errorf("unsupported scheme '%s'", u.Scheme)
	}
}

func parseURL(url string) (*_url.URL, error) {
	u, err := _url.Parse(url)
	if err != nil {
		return nil, err
	}
	var port int
	switch u.Scheme {
	case "tcp":
		port = 1883
	case "ssl":
		port = 8883
	default:
		return nil, fmt.Errorf("invalid")
	}
	if u.Port() != "" {
		return u, nil
	}
	u.Host = fmt.Sprintf("%s:%d", u.Host, port)
	return u, nil
}

func decodeMessage(pub *paho.Publish) (*Message, error) {
	msg := &Message{
		Received: time.Now().UTC(),
		Topic:    pub.Topic,
	}
	if err := json.Unmarshal(pub.Payload, &msg.Payload); err != nil {
		return msg, fmt.Errorf("invalid json: %w", err)
	}

	// decode b64 encoded integrity value
	val, err := base64.StdEncoding.DecodeString(msg.Payload.Integrity.Value)
	if err != nil {
		return msg, fmt.Errorf("could not decode b64 integrity value")
	}
	msg.Payload.Integrity.Method = strings.ToLower(msg.Payload.Integrity.Method)
	msg.Payload.Integrity.Value = string(val)

	return msg, nil
}

type MQTTReceiverOpt func(*MQTTReceiver)

func WithTLSConfig(cfg *tls.Config) MQTTReceiverOpt {
	return func(r *MQTTReceiver) {
		r.tlsConfig = cfg
	}
}

func WithEnvCredentials(pfx string) MQTTReceiverOpt {
	return func(r *MQTTReceiver) {
		if v, ok := os.LookupEnv(pfx + "_USER"); ok {
			r.user = v
		}
		if v, ok := os.LookupEnv(pfx + "_PASSWD"); ok {
			r.user = v
		}
	}
}

func WithClientID(id string) MQTTReceiverOpt {
	return func(r *MQTTReceiver) {
		r.clientID = id
	}
}

func WithQoS(qos byte, cleanStart bool) MQTTReceiverOpt {
	return func(r *MQTTReceiver) {
		r.qos = qos
		r.cleanStart = cleanStart
	}
}

func WithDebug(debug bool) MQTTReceiverOpt {
	return func(r *MQTTReceiver) {
		r.debug = debug
	}
}

func WithIgnoreTopicErrors(b bool) MQTTReceiverOpt {
	return func(r *MQTTReceiver) {
		r.ignoreTopicErrors = b
	}
}

type MQTTReceiver struct {
	log   *log.Logger
	debug bool

	url               string
	clientID          string
	user, passwd      string
	keepAlive         uint16
	cleanStart        bool
	qos               byte
	ignoreTopicErrors bool
	tlsConfig         *tls.Config

	client      *paho.Client
	publishings chan *paho.Publish
	cur         *Message
	err         error
}

func NewMQTTReceiver(ctx context.Context, brokerURL string, topics []string, opts ...MQTTReceiverOpt) (Receiver, error) {
	recv := &MQTTReceiver{
		url:         brokerURL,
		log:         log.New(os.Stdout, "[broker] ", log.LstdFlags),
		keepAlive:   30,
		cleanStart:  false,
		qos:         1,
		publishings: make(chan *paho.Publish),
	}

	for _, o := range opts {
		o(recv)
	}

	if err := recv.createClient(); err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}
	if err := recv.connect(ctx); err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	if err := recv.subscribe(topics); err != nil {
		return nil, fmt.Errorf("subscribing: %w", err)
	}

	return recv, nil
}

func (r *MQTTReceiver) createClient() error {
	conn, err := connectTCP(r.url, r.tlsConfig)
	if err != nil {
		return err
	}
	c := paho.NewClient(paho.ClientConfig{
		ClientID: r.clientID,
		Conn:     conn,
		Router: paho.NewSingleHandlerRouter(func(m *paho.Publish) {
			r.publishings <- m
		}),
	})
	c.SetErrorLogger(r.log)
	if r.debug {
		r.client.SetDebugLogger(r.log)
	}
	return nil
}

func (r *MQTTReceiver) connect(ctx context.Context) error {
	req := &paho.Connect{
		KeepAlive:  r.keepAlive,
		ClientID:   r.clientID,
		CleanStart: r.cleanStart,
		Username:   r.user,
		Password:   []byte(r.passwd),
	}
	req.UsernameFlag = r.user != ""
	req.PasswordFlag = r.passwd != ""
	resp, err := r.client.Connect(ctx, req)
	// Docs are indicate there may be a connack if there is an error
	if resp != nil && err != nil {
		return fmt.Errorf("[%v] %s: %w", resp.ReasonCode, resp.Properties.ReasonString, err)
	} else if err != nil {
		return err
	}
	if resp.ReasonCode != 0 {
		return fmt.Errorf("[%v] %s", resp.ReasonCode, resp.Properties.ReasonString)
	}

	go func() {
		<-ctx.Done()
		r.client.Disconnect(&paho.Disconnect{ReasonCode: 0})
	}()

	return nil
}

func (r *MQTTReceiver) subscribe(topics []string) error {
	// MQTT Subscribe
	scrips := map[string]paho.SubscribeOptions{}
	for _, topic := range topics {
		scrips[topic] = paho.SubscribeOptions{QoS: byte(r.qos)}
	}
	sa, err := r.client.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: scrips,
	})
	if err != nil {
		return err
	}
	failed := []string{}
	for idx, reason := range sa.Reasons {
		if reason != byte(r.qos) && !r.ignoreTopicErrors {
			failed = append(failed, topics[idx])
		}
	}
	if err != nil {
		return fmt.Errorf("creating subscriptions: %w", err)
	}
	if len(failed) > 0 {
		return &TopicsError{Failed: failed}
	}
	return nil
}

func (r *MQTTReceiver) Message() *Message { return r.cur }
func (r *MQTTReceiver) Err() error        { return r.err }
func (r *MQTTReceiver) Next() bool {
	if r.err != nil {
		return false
	}
	pub, ok := <-r.publishings
	if !ok {
		r.cur = nil
		return false
	}
	r.cur, r.err = decodeMessage(pub)
	return r.err == nil
}

var _ Receiver = (*MQTTReceiver)(nil)
