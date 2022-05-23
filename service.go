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
	log     *internal.Logger
	verbose bool

	receiver internal.Receiver
	repo     internal.Repo
	executor internal.Executor
	command  string
}

func newService(recv internal.Receiver, repo internal.Repo, command string, verbose bool) service {
	return service{
		log:     internal.NewLogger(verbose),
		verbose: verbose,

		receiver: recv,
		repo:     repo,
		executor: defaultExecutor,
		command:  command,
	}
}

// validateMessage returns an error if the message likely will not be able to
// be downloaded, nil if it's ok to try an ingest.
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

func (svc *service) Run(ctx context.Context, numWorkers int) error {
	wg := &sync.WaitGroup{}
	tasks := make(chan task)
	results := make(chan taskResult)

	// Start the workers consuming from the work queue and populating the results queue.
	// Workers will run until the source messages channel is closed which will occur when
	// the upstream broker closes its message channel or the main context is canceled.
	svc.log.Debug("starting %d download workers", numWorkers)
	workerWg := &sync.WaitGroup{}
	for i := 0; i < numWorkers; i++ {
		workerWg.Add(1)
		go svc.worker(workerWg, tasks, results)
	}

	// Put tasks on the work queue
	wg.Add(1)
	go func() {
		defer close(tasks)
		defer wg.Done()
		for svc.receiver.Next() {
			msg := svc.receiver.Message()
			svc.log.Info("received topic='%s' url='%s'", msg.Topic, msg.Payload.URL())
			if err := svc.validateMessage(msg); err != nil {
				svc.log.Info("skipping! %s", err)
				continue
			}
			svc.log.Debug("submitting: %+v", msg)
			tasks <- task{msg: msg, repo: svc.repo}
		}
		svc.log.Debug("no more work")
	}()

	// Handle the task results
	wg.Add(1)
	go func() {
		defer wg.Done()
		for zult := range results {
			f := zult.Result
			topic := zult.Result.msg.Topic
			url := zult.Result.msg.Payload.URL()
			if zult.Err != nil {
				svc.log.Error("ingest failed topic='%s' url='%s': %s", url, topic, zult.Err)
				continue
			}

			svc.log.Info("ingested %s to %s in %v", url, f.path, zult.Finished.Sub(zult.Started))
			if svc.command == "" {
				continue
			}
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			svc.log.Debug("executing '%s %s %s'", svc.command, topic, f.path)
			if err := svc.executor(ctx, svc.command, topic, f.path); err != nil {
				svc.log.Error("command failed on %s: %s", f.path, err)
			}
			cancel()
		}
		svc.log.Debug("no more results")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// Make sure workers exit cleanly
		workerWg.Wait()
		svc.log.Debug("all workers have finished")
		close(results)
	}()

	wg.Wait()

	return nil
}

type task struct {
	repo internal.Repo
	msg  *internal.Message
}

type taskResult struct {
	Result            ingestResult
	Err               error
	Started, Finished time.Time
}

func (svc service) worker(wg *sync.WaitGroup, in <-chan task, out chan<- taskResult) {
	defer wg.Done()
	for task := range in {
		i, err := ingestOne(task.msg, task.repo)
		out <- taskResult{
			Result: i,
			Err:    err,
		}
	}
	svc.log.Debug("worker exiting")
}

type ingestResult struct {
	msg  *internal.Message
	path string
}

func ingestOne(msg *internal.Message, repo internal.Repo) (ingestResult, error) {
	wis := msg.Payload
	url := wis.URL()
	zult := ingestResult{msg: msg}
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

	zult.path, err = repo.Store(msg.Topic, tmp.Name())
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
