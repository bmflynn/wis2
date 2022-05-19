package internal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Repo interface {
	Store(topic, src string) (string, error)
	Get(topic, name string) (*os.File, error)
	Exists(topic, name string) (bool, error)
}

type FSRepo struct {
	root string
}

func (fs *FSRepo) path(topic, name string) string {
	return filepath.Join(fs.root, filepath.Base(name))
}

func (fs *FSRepo) Store(topic, fpath string) (string, error) {
	dir := filepath.Join(fs.root, filepath.FromSlash(topic))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	src, err := os.Open(fpath)
	if err != nil {
		return "", fmt.Errorf("opening source: %w", err)
	}
	dstPath := fs.path(topic, fpath)
	dst, err := os.Create(dstPath)
	if err != nil {
		return "", fmt.Errorf("creating %s: %w", dstPath, err)
	}
	_, err = io.Copy(dst, src)
	return dstPath, err
}

func (fs *FSRepo) Get(topic, name string) (*os.File, error) {
	return os.Open(fs.path(topic, name))
}

func (fs *FSRepo) Exists(topic, name string) (bool, error) {
	_, err := fs.Get(topic, name)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

var _ Repo = (*FSRepo)(nil)

func NewRepo(path string) (Repo, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("path is not a dir")
	}
	return &FSRepo{root: path}, nil
}
