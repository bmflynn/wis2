package internal

import (
	"context"
	"io"
	"net/http"
	_url "net/url"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/jdxcode/netrc"
)

var defaultCredentialGetter CredentialGetter

func init() {
	defaultCredentialGetter = newNetrcCredentialFactory("")
}

type Fetcher interface {
	// Fetch the file at url to dest. They file will exist at dest unless err is non-nil.
	Fetch(url string, dst io.Writer) error
	// FetchContext is the same as Fetch but takes a context to potentially cancel the fetch.
	FetchContext(ctx context.Context, url string, dst io.Writer) error
}

type FetcherFactory func(url string) Fetcher

// FindFetcher returns a fetcher for the URL, if available, otherwise nil.
func FindFetcher(url string) Fetcher {
	u, err := _url.Parse(url)
	if err != nil {
		return nil
	}
	switch u.Scheme {
	case "http", "https":
		return &HTTPFetcher{client: *http.DefaultClient}
	case "ftp":
		return &FTPFetcher{}
	}
	return nil
}

type CredentialGetter func(host string) (string, string, error)

func newNetrcCredentialFactory(fpath string) CredentialGetter {
	if fpath == "" {
		locations := []string{}

		// From ENV
		if v, ok := os.LookupEnv("NETRC"); ok {
			locations = append(locations, v)
		}
		defaultName := ".netrc"
		if runtime.GOOS == "windows" {
			defaultName = "_netrc"
		}

		// In CWD
		cwd, err := os.Getwd()
		if err == nil {
			locations = append(locations, filepath.Join(cwd, defaultName))
		} else {
			locations = append(locations, defaultName)
		}

		// In $HOME
		usr, err := user.Current()
		if err == nil {
			locations = append(locations, filepath.Join(usr.HomeDir), defaultName)
		}

		// Use the first location that exists
		for _, loc := range locations {
			_, err := os.Stat(loc)
			if err != nil {
				continue
			}
			fpath = loc
			break
		}
	}
	return func(host string) (string, string, error) {
		host, _, _ = strings.Cut(host, ":") // strip port

		n, err := netrc.Parse(fpath)
		if err != nil {
			return "", "", err
		}
		m := n.Machine(host)
		if m == nil {
			return "", "", nil
		}
		return m.Get("login"), m.Get("password"), nil
	}
}
