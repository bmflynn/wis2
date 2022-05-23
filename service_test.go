package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/bmflynn/wis2/internal"
)

type mockReceiver struct {
	idx      int
	messages []*internal.Message
	err      error
}

func (r *mockReceiver) Message() *internal.Message {
	if r.messages == nil {
		return nil
	}
	msg := r.messages[r.idx]
	r.idx++
	if r.idx >= len(r.messages) {
		r.messages = nil
	}
	return msg
}
func (r *mockReceiver) Next() bool { return r.messages != nil }
func (r *mockReceiver) Err() error { return r.err }

func newMockExecutor(err error) internal.Executor {
	return func(ctx context.Context, name string, args ...string) error {
		return err
	}
}

type mockRepo struct {
	exists bool
	err    error
	stored map[string]string
}

func (r *mockRepo) Store(topic string, name string) (string, error) { return "<nope>", r.err }
func (r *mockRepo) Get(topic string, name string) (*os.File, error) {
	return os.CreateTemp("", "")
}
func (r *mockRepo) Exists(topic string, name string) (bool, error) { return r.exists, r.err }

func newMockRepo(t *testing.T) internal.Repo {
	t.Helper()

	return &mockRepo{stored: map[string]string{}}
}

type mockFetcher struct{}

func (f *mockFetcher) FetchContext(ctx context.Context, url string, dst io.Writer) error {
	buf := &bytes.Buffer{}
	buf.WriteString(url)
	_, err := io.Copy(dst, buf)
	return err
}
func (f *mockFetcher) Fetch(url string, dst io.Writer) error {
	return f.FetchContext(context.Background(), url, dst)
}

func newStaticFetcherFactory(f internal.Fetcher) internal.FetcherFactory {
	return func(url string) internal.Fetcher {
		return f
	}
}

func TestService(t *testing.T) {
	defaultFetcherFactory = newStaticFetcherFactory(&mockFetcher{})
	now := time.Now()
	svc := service{
		verbose: true,
		receiver: &mockReceiver{
			messages: []*internal.Message{
				{
					Topic:    "a/b/c",
					Received: time.Now(),
					Source:   "",
					Payload: internal.WISMessage{
						PubTime: &now,
						BaseURL: "test://foo",
						RelPath: "path/file.ext",
						Integrity: internal.Integrity{
							Method: "md5",
							Value:  "d41d8cd98f00b204e9800998ecf8427e",
						},
					},
				},
			},
		},
		repo:     newMockRepo(t),
		executor: newMockExecutor(nil),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	err := svc.Run(ctx, 1)
	if err != nil {
		t.Errorf("got err %s", err)
	}
}
