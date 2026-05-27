package xfs

import (
	"sync"

	"github.com/avatar31/omashu"

	"github.com/avatar31/dotfs/config"
)

type XFSStorage struct {
	engine *StorageEngine
	drives []Drive

	MetaDB *omashu.Badger
}

var (
	fs   *XFSStorage
	once sync.Once
)

func NewXFSStorage(conf config.Config, metaDB *omashu.Badger) (*XFSStorage, error) {
	var err error
	once.Do(func() {
		var drives []Drive
		var engine *StorageEngine
		engine, err = NewStorageEngine(conf.DataShardCount, conf.ParityShardCount, drives)
		if err != nil {
			// TODO: Log error
			return
		}

		fs = &XFSStorage{
			engine: engine,
			drives: drives,
			MetaDB: metaDB,
		}
	})

	return fs, err
}

func (xfs *XFSStorage) GetMetaDB() *omashu.Badger {
	return xfs.MetaDB
}
