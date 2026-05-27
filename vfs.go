package dotfs

import (
	"context"
	"fmt"

	"github.com/avatar31/omashu"

	"github.com/avatar31/dotfs/config"
	"github.com/avatar31/dotfs/fsal/xfs"
)

type VFS interface {
	// Add all methods of linux VFS here
	// This should support all NFS Gateway operations
	GetMetaDB() *omashu.Badger
}

func NewVFS(ctx context.Context, cfg config.Config) (VFS, error) {
	db, err := omashu.NewBadger(ctx, cfg.OmashuConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create meta database: %w", err)
	}

	switch cfg.BaseFS {
	case config.XFS:
		return xfs.NewXFSStorage(cfg, db)
	default:
		return nil, fmt.Errorf("unsupported base filesystem: %s", cfg.BaseFS)
	}
}
