package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/bmflynn/wis2/internal"
	"github.com/spf13/pflag"
)

var version = "<notset>"

func init() {
	flags := pflag.CommandLine
	flags.BoolP("help", "h", false, "Show help and exit")
	flags.Bool("verbose", false, "Verbose output")
	flags.Bool("version", false, "Show version and exit")

	flags.StringP("broker", "b", os.Getenv("WIS2_BROKER"),
		"MQTT broker URL as either ssl://<host>[:<port>] for MQTT over TLS or tcp://<host>[:<port>] "+
			"for MQTT without TLS. If the port is not specified the MQTT standard port numbers 8883 "+
			"and 1883 will be used.",
	)
	flags.StringSliceP("topic", "t", nil, "Topic to subscribe to. May be specified multiple times or as CSV.")
	flags.Bool("ignore-topic-errors", false,
		"Ignore errors that occur when subscribing to topics. By default a subscription failure for "+
			"any topic is fatal.")

	flags.IntP("workers", "w", 4, "Maximum number of files to download concurrently.")
	flags.StringP("datadir", "d", "data", "Directory to store data")

	flags.String("command", "",
		"A script or command to execute for every file successfully ingested file. The command must take "+
			"the topic and the local file path as arguments. Command failures are logged, but not fatal. "+
			"The command should be very simple and execute quickly to avoid clogging up message "+
			"consumption. Commands are run sequentially after files are downloaded.")

	pflag.Usage = usage
}

func chkflag(err error) {
	if err != nil {
		panic(err)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Services for downloading products diseminated WMO WIS2. 
	
Usage: %s [flags] --broker=<broker> --topic=<topic> [--topic=...]

Broker credentials are specified using the WIS2_(USER|PASSWD) environment variables.

Data will be downloaded to the directory provided by --datadir in directories matching
the topic. 

Flags
`, filepath.Base(os.Args[0]))
	pflag.PrintDefaults()
}

func Execute() error {
	pflag.Parse()
	flags := pflag.CommandLine

	help, err := flags.GetBool("help")
	chkflag(err)
	if help {
		pflag.Usage()
    return nil
	}

	showVer, err := flags.GetBool("version")
	chkflag(err)
	if showVer {
		fmt.Println(filepath.Base(os.Args[0]), version)
    return nil
	}

	brokerURL, err := flags.GetString("broker")
	chkflag(err)
	if brokerURL == "" {
		return fmt.Errorf("--broker must be specified")
	}
	topics, err := flags.GetStringSlice("topic")
	chkflag(err)
	if len(topics) == 0 {
		return fmt.Errorf("no topics specified")
	}
	ignoreTopicErrs, err := flags.GetBool("ignore-topic-errors")
	chkflag(err)
	dataDir, err := flags.GetString("datadir")
	chkflag(err)
	verbose, err := flags.GetBool("verbose")
	chkflag(err)
	command, err := flags.GetString("command")
	chkflag(err)
	workers, err := flags.GetInt("workers")
	chkflag(err)

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		log.Printf("signal '%s'", <-ch)
		cancel()
	}()

	// The new broker will be cleanly disconnected iff the context is canceled.
	receiver, err := internal.NewMQTTReceiver(
		ctx, brokerURL, topics,
		internal.WithIgnoreTopicErrors(ignoreTopicErrs),
		internal.WithEnvCredentials("WIS"),
	)
	if err != nil {
		log.Fatalf("failed to create message receiver: %s", err)
	}

	repo, err := internal.NewRepo(dataDir)
	if err != nil {
		log.Fatalf("failed to create data repository: %s", err)
	}

	service := newService(receiver, repo, command, verbose)
	if err := service.Run(ctx, workers); err != nil {
		log.Fatalf("failed! %s", err)
	}

  return nil
}

func main() {
  if err := Execute(); err != nil {
    fmt.Printf("ERROR: %s\n", err)
    os.Exit(2)
  }
}
