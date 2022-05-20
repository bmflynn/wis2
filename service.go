package main

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/bmflynn/wis2/internal"
)

var (
	defaultFetcherFactory internal.FetcherFactory = internal.FindFetcher
	defaultExecutor       internal.Executor       = internal.RunScript
)

type service struct {
	errLog   *log.Logger
	debugLog *log.Logger
	log      *log.Logger
	verbose  bool

	receiver internal.Receiver
	repo     internal.Repo
	executor internal.Executor
	command  string
}

func newService(recv internal.Receiver, repo internal.Repo, command string, verbose bool) service {
	return service{
		errLog:   log.New(os.Stderr, "ERROR: ", log.LstdFlags),
		debugLog: log.New(os.Stderr, "DEBUG: ", log.LstdFlags|log.Lshortfile),
		log:      log.Default(),
		verbose:  verbose,

		receiver: recv,
		repo:     repo,
		executor: defaultExecutor,
		command:  command,
	}
}

func (svc *service) validateMessage(msg *internal.Message) error {
	// Verify message is valid
	topic := msg.Topic
	name := path.Base(msg.Payload.RelPath)
	if err := msg.Payload.IsValid(); err != nil {
		return fmt.Errorf("invalid; topic='%s' name='%s' message='%+v' %s", topic, name, msg, err)
	}
	// Verify it doesn't already exist
	exists, err := svc.repo.Exists(topic, name)
	if err != nil {
		return fmt.Errorf("failed to execute exists check, skipping!: %s", err)
	}
	if exists {
		return fmt.Errorf("skipping! exists locally topic='%s' name='%s'", topic, name)
	}
	return nil
}

func (svc *service) debugf(fmt string, args ...interface{}) {
	if svc.verbose {
		svc.debugLog.Printf(fmt, args...)
	}
}

func (svc *service) errorf(fmt string, args ...interface{}) {
	svc.errLog.Printf(fmt, args...)
}

func (svc *service) Run(ctx context.Context, numWorkers int) error {
	work := make(chan internal.Work)
	results := make(chan internal.WorkResult)

	// Put Work functions on the work queue
	go func() {
		for svc.receiver.Next() {
			msg := svc.receiver.Message()
			if err := svc.validateMessage(msg); err != nil {
				svc.log.Printf("skipping! %s", err)
			}
			svc.debugf("submitting for ingest: %+v", msg)
			work <- func() (any, error) {
				return ingestOne(ctx, msg, svc.repo)
			}
		}
	}()

	// Start the workers consuming from the work queue and populating the results queue.
	// Workers will run until the source messages channel is closed which will occur when
	// the upstream broker closes its message channel or the main context is canceled.
	svc.debugf("starting %d download workers", numWorkers)
	wg := &sync.WaitGroup{}
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		worker := internal.NewWorker(i, work, results)
		go worker.Run(wg)
	}

	// Handle results serially
	for zult := range results {
		f := zult.Result.(ingestFile)
		svc.log.Printf("ingested %s to %s in %v", f.Source, f.Dest, zult.Finished.Sub(zult.Started))
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		svc.debugf("executing '%s %s %s'", svc.command, f.Topic, f.Dest)
		if err := svc.executor(ctx, svc.command, f.Topic, f.Dest); err != nil {
			svc.errorf("command failed on %s: %s", f.Dest, err)
		}
		cancel()
	}

	// Make sure workers exit cleanly
	wg.Wait()

	return nil
}

type ingestFile struct {
	Topic  string
	Source string
	Dest   string
}

func ingestOne(ctx context.Context, msg *internal.Message, repo internal.Repo) (ingestFile, error) {
	wis := msg.Payload
	url := wis.URL()
	zult := ingestFile{Topic: msg.Topic, Source: url}
	fetcher := defaultFetcherFactory(url)

	// Fetch the file to a temporary location
	tmp, err := os.CreateTemp("", path.Base(url)+".*")
	if err != nil {
		return zult, fmt.Errorf("creating tmp: %w", err)
	}
	defer tmp.Close()
	if err := fetcher.Fetch(url, tmp); err != nil {
		return zult, fmt.Errorf("fetching: %w", err)
	}
	tmp.Sync() // make sure it's all written to disk

	// verify checksum
	if err := verifyWISChecksum(wis.Integrity.Method, wis.Integrity.Value, tmp); err != nil {
		return zult, fmt.Errorf("integrity check failed: %w", err)
	}

	zult.Dest, err = repo.Store(msg.Topic, tmp.Name())
	return zult, err
}

func verifyWISChecksum(method, expected string, r io.Reader) error {
	method = strings.ToLower(method)
	var alg hash.Hash
	switch method {
	case "md5":
		alg = md5.New()
	case "sha512":
		alg = sha512.New()
	case "sha256":
		alg = sha256.New()
	default:
		return fmt.Errorf("unsupported method '%s'", method)
	}

	if _, err := io.Copy(alg, r); err != nil {
		return fmt.Errorf("executing; method='%s': %w", method, err)
	}

	got := hex.EncodeToString(alg.Sum(nil))
	if got != expected {
		return fmt.Errorf("mismatch; method='%s' expected='%s' got='%s'", method, expected, got)
	}
	return nil
}
