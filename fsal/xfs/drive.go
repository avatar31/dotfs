package xfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Drive abstracts physical or logical storage that can hold shard data.
type Drive interface {
	Write(ctx context.Context, path string, data []byte) error
	Read(ctx context.Context, path string, dest []byte) (int, error)
	GetPath() string
	GetID() int64
}

// LocalDrive is a concrete Drive backed by the local filesystem (stub).
type LocalDrive struct {
	id   int64
	path string
}

func NewLocalDrive(id int64, path string) *LocalDrive {
	return &LocalDrive{id: id, path: path}
}

func (d *LocalDrive) Write(ctx context.Context, path string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %q: %w", dir, err)
	}

	tmp, err := os.CreateTemp(dir, ".shard_*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file in %q: %w", dir, err)
	}
	tmpName := tmp.Name()

	committed := false
	defer func() {
		if !committed {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()

	if _, err = tmp.Write(data); err != nil {
		return fmt.Errorf("failed to write shard data to %q: %w", tmpName, err)
	}

	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("failed to fsync %q: %w", tmpName, err)
	}

	if err = tmp.Close(); err != nil {
		return fmt.Errorf("failed to close temp file %q: %w", tmpName, err)
	}

	if err = ctx.Err(); err != nil {
		return err
	}

	if err = os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("failed to commit shard %q: %w", path, err)
	}

	// Fsync parent directory to lock directory metadata entries into disk storage
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("failed to open parent directory for sync %q: %w", dir, err)
	}
	defer dirFile.Close()

	if err = dirFile.Sync(); err != nil {
		return fmt.Errorf("failed to fsync parent directory metadata %q: %w", dir, err)
	}

	committed = true
	return nil
}

func (d *LocalDrive) Read(ctx context.Context, path string, dest []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	file, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("failed to open shard %q: %w", path, err)
	}
	defer file.Close()

	n, err := io.ReadFull(file, dest)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return n, fmt.Errorf("failed to read full shard %q payload: %w", path, err)
	}

	return n, nil
}

func (d *LocalDrive) GetPath() string {
	return d.path
}

func (d *LocalDrive) GetID() int64 {
	return d.id
}

// memDrive is an in-memory Drive implementation for testing.
type memDrive struct {
	id    int64
	path  string
	mu    sync.RWMutex
	store map[string][]byte
}

func newMemDrive(id int64, path string) *memDrive {
	return &memDrive{id: id, path: path, store: make(map[string][]byte)}
}

func (d *memDrive) Write(_ context.Context, path string, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	buf := make([]byte, len(data))
	copy(buf, data)
	d.store[path] = buf

	// fmt.Printf("memDrive: wrote %d bytes to path %s\n", len(data), path)
	return nil
}

func (d *memDrive) Read(_ context.Context, path string, dst []byte) (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	data, ok := d.store[path]
	if !ok {
		return 0, fmt.Errorf("shard not found: %s", path)
	}

	copy(dst, data)

	// fmt.Printf("memDrive: read %d bytes from path %s\n", len(dst), path)
	return len(dst), nil
}

func (d *memDrive) GetPath() string {
	return d.path
}

func (d *memDrive) GetID() int64 {
	return d.id
}
