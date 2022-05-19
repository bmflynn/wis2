package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/bmflynn/wis2/internal"
	"github.com/spf13/pflag"
)

func init() {
	flags := pflag.CommandLine
	flags.StringP("broker", "b", os.Getenv("WIS2_BROKER"),
		"MQTT broker URL as either ssl://<host>[:<port>] for MQTT over TLS or tcp://<host>[:<port>] "+
			"for MQTT without TLS. If the port is not specified the MQTT standard port numbers 8883 "+
			"and 1883 will be used.",
	)
	flags.StringSliceP("topic", "t", nil, "Topic to subscribe to. May be specified multiple times or as CSV.")
	flags.Bool("ignore-topic-errors", false,
		"Ignore errors that occur when subscribing to topics. By default a subscription failure for "+
			"any topic is fatal.")
}

func chkflag(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	flags := pflag.CommandLine
	brokerURL, err := flags.GetString("broker")
	chkflag(err)
	if brokerURL == "" {
		fmt.Println("--broker must be specified")
		os.Exit(1)
	}
	topics, err := flags.GetStringSlice("topic")
	chkflag(err)
	if len(topics) == 0 {
		fmt.Println("no topics specified")
		os.Exit(1)
	}
	ignoreTopicErrs, err := flags.GetBool("ignore-topic-errors")
	chkflag(err)

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		log.Printf("signal '%s'", <-ch)
		cancel()
	}()

	if err := run(ctx, brokerURL, topics, ignoreTopicErrs); err != nil {
		log.Fatalf("failed! %s", err)
	}
}

func newBroker(ctx context.Context, brokerURL string, topics []string, ignoreTopicErrs bool) (*internal.Broker, error) {
	opts := internal.NewDefaultBrokerOptions()
	opts.SetCredentialsFromEnv("WIS2")
	broker, err := internal.NewBroker(brokerURL, opts)
	if err != nil {
		return nil, fmt.Errorf("init: %w", err)
	}
	failures, err := broker.Subscribe(topics...)
	if err != nil {
		return nil, fmt.Errorf("creating subscriptions: %w", err)
	}
	if len(failures) > 0 {
		msg := fmt.Sprintf("failed to subscribe to topics %s", strings.Join(failures, ","))
		if ignoreTopicErrs {
			log.Printf("WARNING: %s", msg)
		} else {
			return nil, fmt.Errorf(msg)
		}
	}
	return broker, nil
}

func run(ctx context.Context, brokerURL string, topics []string, ignoreTopicErrs bool) error {
	broker, err := newBroker(ctx, brokerURL, topics, ignoreTopicErrs)
	if err != nil {
		return fmt.Errorf("broker: %w", err)
	}

}
