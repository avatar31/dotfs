package config

import "github.com/avatar31/omashu"

type BaseFS string

const (
	// Base Filesystem types
	XFS BaseFS = "xfs"
)

type Config struct {
	DataShardCount   int
	ParityShardCount int
	BaseFS           BaseFS
	Drives           []string

	OmashuConfig *omashu.Config
}
