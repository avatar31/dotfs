package models

const (
	ShardTypeData   = "data"
	ShardTypeParity = "parity"
)

type ObjectChunkShard struct {
	Index        int    `json:"index"`
	DriveId      int64  `json:"driveId"`
	Path         string `json:"path"`
	Type         string `json:"type"`
	LogicalSize  int64  `json:"logicalSize"`
	PhysicalSize int64  `json:"physicalSize"`
}

type ObjectChunk struct {
	ID           int64               `json:"id"`
	Start        int64               `json:"start"`
	End          int64               `json:"end"`
	LogicalSize  int64               `json:"logicalSize"`
	PhysicalSize int64               `json:"physicalSize"`
	Shards       []*ObjectChunkShard `json:"shards"`
}

type ObjectStorageMeta struct {
	ObjectId string         `json:"objectId"`
	Size     int64          `json:"size"`
	Filename string         `json:"filename"`
	Path     string         `json:"path"`
	Chunks   []*ObjectChunk `json:"chunks"`
}
