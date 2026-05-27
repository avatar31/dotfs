package config

import "github.com/avatar31/omashu"

type BaseFS string

const (
	XFS BaseFS = "xfs"
)

type Config struct {
	DataShardCount   int
	ParityShardCount int
	BaseFS           BaseFS

	OmashuConfig *omashu.Config
}
