package internal

import (
	"crypto/rand"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func fixtureDir(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Errorf("failed to create tmpdir: %s", err)
	}
	return dir, func() { os.RemoveAll(dir) }
}

func fixtureFile(t *testing.T) (*os.File, func()) {
	t.Helper()

	f, err := os.CreateTemp("", "")
	if err != nil {
		t.Errorf("failed to create tmpfile: %s", err)
	}
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		t.Errorf("failed to read random: %s", err)
	}
	if _, err = f.Write(buf); err != nil {
		t.Errorf("failed to write rand: %s", err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		t.Errorf("failed to seek: %s", err)
	}

	return f, func() { os.Remove(f.Name()) }
}

func TestFSRepo(t *testing.T) {
	dir, cleanup := fixtureDir(t)
	defer cleanup()
	f, cleanup := fixtureFile(t)
	defer cleanup()

	repo, err := NewRepo(dir)
	if err != nil {
		t.Errorf("failed to create repo: %s", err)
	}

	t.Run("Store", func(t *testing.T) {
		gotPath, err := repo.Store("foo/goo", f.Name())
		if err != nil {
			t.Errorf("failed to store file: %s", err)
		}
		expectedPath := filepath.Join(dir, "foo/goo", filepath.Base(f.Name()))
		if expectedPath != gotPath {
			t.Errorf("got path %s, expected %s", gotPath, expectedPath)
		}
	})

	t.Run("Exists", func(t *testing.T) {
		exists, err := repo.Exists("foo/goo", f.Name())
		if err != nil {
			t.Errorf("expected no error, got %s", err)
		}
		if !exists {
			t.Errorf("expected exists flag to be true for %s, got %v", f.Name(), exists)
		}
	})

	t.Run("Get", func(t *testing.T) {
		gotF, err := repo.Get("foo/goo", f.Name())
		if err != nil {
			t.Errorf("expected no error, got %s", err)
		}
		got, err := ioutil.ReadAll(gotF)
		if err != nil {
			t.Errorf("failed to read returned file: %s", err)
		}
		expected, err := ioutil.ReadAll(f)
		if err != nil {
			t.Errorf("failed to read source file: %s", err)
		}
		if len(got) != len(expected) {
			t.Errorf("expected %v bytes, got %v", len(expected), len(got))
		}
		for i := 0; i < len(expected); i++ {
			if got[i] != expected[i] {
				t.Errorf("expected %v at byte %v, got %v", expected[i], i, got[i])
			}
		}
	})
}
