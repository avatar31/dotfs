package dotfs

type BaseFS int

const (
	XFS BaseFS = iota
)

type Config struct {
	DataShardCount   int
	ParityShardCount int
	BaseFS           BaseFS
}
