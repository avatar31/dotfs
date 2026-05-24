package xfs

import "context"

type Drive struct {
	id   int
	Path string
}

func (d *Drive) Write(ctx context.Context, path string, data []byte) error {
	return nil
}

func (d *Drive) GetPath() string {
	return d.Path
}
