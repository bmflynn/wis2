package internal

import (
	"context"
	"fmt"
	"io"
	"net/http"
	_url "net/url"
)

type HTTPFetcher struct {
	client http.Client
}

func (f *HTTPFetcher) FetchContext(ctx context.Context, url string, dst io.Writer) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("creating req: %w", err)
	}
	req = req.WithContext(ctx)

	u, err := _url.Parse(url)
	if err != nil {
		return err
	}
	user, passwd, err := defaultCredentialGetter(u.Host)
	if err != nil {
		return fmt.Errorf("unable to load credentials")
	}
	if user != "" || passwd != "" {
		req.SetBasicAuth(user, passwd)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(dst, resp.Body)
	return err
}

func (f *HTTPFetcher) Fetch(url string, dst io.Writer) error {
	return f.FetchContext(context.Background(), url, dst)
}

var _ Fetcher = (*HTTPFetcher)(nil)
