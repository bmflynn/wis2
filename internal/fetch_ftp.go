package internal

import (
	"context"
	"fmt"
	"io"
	_url "net/url"
	"time"

	"github.com/jlaffaye/ftp"
)

// FTPFetcher is a Fetcher for downloading ftp:// URLs. There is no connection caching,
// so every fetch creates a new FTP connection.
type FTPFetcher struct{}

func (f *FTPFetcher) FetchContext(ctx context.Context, url string, dst io.Writer) error {
	u, err := _url.Parse(url)
	if err != nil {
		return err
	}
	if u.Scheme != "ftp" {
		return fmt.Errorf("invalid FTP url")
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	conn, err := ftp.Dial(u.Host, ftp.DialWithContext(ctx))
	if err != nil {
		return err
	}
	defer conn.Quit()

	user, passwd, err := defaultCredentialGetter(u.Host)
	if err != nil {
		return fmt.Errorf("unable to load credentials")
	}
	if user != "" || passwd != "" {
		if err := conn.Login(user, passwd); err != nil {
			return err
		}
	}

	resp, err := conn.Retr(u.Path)
	if err != nil {
		return err
	}
	defer resp.Close()

	_, err = io.Copy(dst, resp)
	return err
}

func (f *FTPFetcher) Fetch(url string, dst io.Writer) error {
	return f.FetchContext(context.Background(), url, dst)
}

var _ Fetcher = (*FTPFetcher)(nil)
