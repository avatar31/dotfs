package xfs

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/klauspost/reedsolomon"

	"github.com/avatar31/dotfs/models"
)

const (
	KiloByte = 1024
	MegaByte = KiloByte * 1024

	MaxChunkSize = 4 * MegaByte // 4 MB fixed chunk size
	CPUCacheLine = 64           // 64 bytes for optimal alignment
)

type StorageEngine struct {
	drives   []Drive
	driveMap map[int64]Drive // Lookup map

	// Erasure Coding Parameters
	rsEncoder    reedsolomon.Encoder
	DataShards   int
	ParityShards int
	TotalShards  int

	matrixPool *sync.Pool
	chunkPool  *sync.Pool
}

func NewStorageEngine(dataShards, parityShards int, drives []Drive) (*StorageEngine, error) {
	totalShards := dataShards + parityShards
	if len(drives) < totalShards {
		return nil, fmt.Errorf("insufficient physical drives mapped to complete erasure layout allocation")
	}

	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}

	maxShardSize := ComputeShardSize(MaxChunkSize, dataShards)
	alignedMaxShardSize := ComputeAlignedShardSize(maxShardSize)

	dMap := make(map[int64]Drive)
	for _, d := range drives {
		dMap[d.GetID()] = d
	}

	return &StorageEngine{
		drives:       drives,
		driveMap:     dMap,
		rsEncoder:    enc,
		DataShards:   dataShards,
		ParityShards: parityShards,
		TotalShards:  totalShards,
		matrixPool: &sync.Pool{
			New: func() any {
				matrix := make([][]byte, totalShards)
				for i := range matrix {
					matrix[i] = make([]byte, alignedMaxShardSize)
				}
				return matrix
			},
		},
		chunkPool: &sync.Pool{
			New: func() any {
				return make([]byte, MaxChunkSize)
			},
		},
	}, nil
}

// EncodeObjectByChunks processes an incoming stream of any size in ChunkSize(4MB) segments.
func (se *StorageEngine) EncodeObjectByChunks(ctx context.Context, meta *models.ObjectStorageMeta,
	data io.Reader) ([]*models.ObjectChunk, error) {
	// A reusable buffer to swallow up to 4MB from the reader at a time
	dataBuffer := se.chunkPool.Get().([]byte)
	defer se.chunkPool.Put(dataBuffer)

	capEstimate := int(meta.Size/MaxChunkSize) + 1
	chunks := make([]*models.ObjectChunk, 0, capEstimate)
	var chunkIdx int64 = 1

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		bytesRead, err := io.ReadFull(data, dataBuffer)
		if err == io.EOF || (err == io.ErrUnexpectedEOF && bytesRead == 0) {
			break
		}

		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("failed to read chunk %d from stream: %w", chunkIdx, err)
		}

		chunk, err := se.EncodeSingleChunk(ctx, meta.Path, meta.Filename, chunkIdx, dataBuffer[:bytesRead])
		if err != nil {
			return nil, err
		}

		chunk.Start = (chunkIdx - 1) * MaxChunkSize
		chunk.End = chunk.Start + int64(bytesRead) - 1
		chunks = append(chunks, chunk)
		chunkIdx++
	}

	return chunks, nil
}

func (se *StorageEngine) EncodeSingleChunk(ctx context.Context, path, filename string, chunkId int64,
	chunkData []byte) (*models.ObjectChunk, error) {
	dataSize := len(chunkData)
	shards := se.matrixPool.Get().([][]byte)
	defer func() {
		for i := range shards {
			shards[i] = shards[i][:cap(shards[i])] // Reset slice length for reuse
		}
		se.matrixPool.Put(shards)
	}()

	minShardSize := ComputeShardSize(dataSize, se.DataShards)
	alignedShardSize := ComputeAlignedShardSize(minShardSize)

	// Safely clear and size ALL shards (Data + Parity) up to aligned size
	for i := range shards {
		if cap(shards[i]) < alignedShardSize {
			shards[i] = make([]byte, alignedShardSize)
		} else {
			shards[i] = shards[i][:alignedShardSize]
		}
		clear(shards[i])
	}

	shardsMeta := make([]*models.ObjectChunkShard, 0, se.TotalShards)

	// Manually split and copy data into shards
	for i := 0; i < se.DataShards; i++ {
		start := i * minShardSize
		if start >= dataSize {
			shardsMeta = append(shardsMeta, &models.ObjectChunkShard{
				Index:        i,
				LogicalSize:  0,
				PhysicalSize: int64(alignedShardSize),
				Type:         models.ShardTypeData,
				Path:         filepath.Join(path, fmt.Sprintf("%s.chunk_%d.shard_%d", filename, chunkId, i)),
			})
			continue
		}
		end := min(start+minShardSize, dataSize)

		copy(shards[i], chunkData[start:end])

		shardsMeta = append(shardsMeta, &models.ObjectChunkShard{
			Index:        i,
			LogicalSize:  int64(end - start),
			PhysicalSize: int64(alignedShardSize),
			Type:         models.ShardTypeData,
			Path:         filepath.Join(path, fmt.Sprintf("%s.chunk_%d.shard_%d", filename, chunkId, i)),
		})
	}

	err := se.rsEncoder.Encode(shards)
	if err != nil {
		return nil, fmt.Errorf("erasure coding computation failed: %w", err)
	}

	for i := se.DataShards; i < se.TotalShards; i++ {
		shardsMeta = append(shardsMeta, &models.ObjectChunkShard{
			Index:        i,
			LogicalSize:  int64(alignedShardSize),
			PhysicalSize: int64(alignedShardSize),
			Type:         models.ShardTypeParity,
			Path:         filepath.Join(path, formatFileName(filename, chunkId, i)),
		})
	}

	err = se.dispatchChunkToPhysicalStorage(ctx, alignedShardSize, shards, shardsMeta)
	if err != nil {
		return nil, fmt.Errorf("chunk dispatch failed: %w", err)
	}

	// TODO: Atomic Commits: Store the chunk shards inside a temporary path (e.g., .tmp/) first.
	// Once all shards return a successful 200 OK or io.EOF, write a small metadata manifest
	// record changing its status to COMMITTED. If an operation fails, the index layer simply
	// ignores uncommitted blocks, and a scavenger sweeps .tmp/ periodically.

	physicalSize := int64(0)
	for i := range shardsMeta {
		physicalSize += shardsMeta[i].PhysicalSize
	}

	return &models.ObjectChunk{
		ID:           chunkId,
		LogicalSize:  int64(len(chunkData)),
		PhysicalSize: physicalSize,
		Shards:       shardsMeta,
	}, nil
}

func (se *StorageEngine) dispatchChunkToPhysicalStorage(ctx context.Context, writeSize int,
	shards [][]byte, shardsMeta []*models.ObjectChunkShard) error {
	gCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errChan := make(chan error, se.TotalShards)

	for i := 0; i < se.TotalShards; i++ {
		if i >= len(se.drives) {
			return fmt.Errorf("insufficient physical drives mapped to complete erasure layout allocation")
		}

		wg.Add(1)
		targetData := shards[i][:writeSize]

		go func(shardIdx int, drive Drive, shardMeta *models.ObjectChunkShard, dataToWrite []byte) {
			defer wg.Done()

			if gCtx.Err() != nil {
				return
			}

			err := drive.Write(gCtx, filepath.Join(drive.GetPath(), shardMeta.Path), dataToWrite)
			if err != nil {
				select {
				case errChan <- fmt.Errorf("drive %d write failed: %w", drive.GetID(), err):
					cancel()
				default:
				}
				return
			}

			shardMeta.DriveId = drive.GetID()
		}(i, se.drives[i], shardsMeta[i], targetData)
	}

	wg.Wait()
	close(errChan)

	if len(errChan) > 0 {
		return <-errChan
	}

	return nil
}

func (se *StorageEngine) ReadObjectByRange(ctx context.Context, meta *models.ObjectStorageMeta, offset, size int64, out io.Writer) error {
	endOffset := offset + size

	for _, chunk := range meta.Chunks {
		// Skip chunks outside the requested byte range
		if chunk.End < offset || chunk.Start >= endOffset {
			continue
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		var shardSize int64
		for _, s := range chunk.Shards {
			if s != nil {
				shardSize = s.PhysicalSize
				break
			}
		}
		if shardSize == 0 {
			return fmt.Errorf("chunk %d has no valid shard metadata", chunk.ID)
		}

		chunkData, err := se.readAndProcessChunkWindow(ctx, chunk, shardSize)
		if err != nil {
			return fmt.Errorf("failed to read chunk %d: %w", chunk.ID, err)
		}

		// Trim to the portion of this chunk that falls within [offset, endOffset)
		writeStart := max(offset-chunk.Start, 0)
		writeEnd := min(endOffset-chunk.Start, chunk.LogicalSize)

		if _, err := out.Write(chunkData[writeStart:writeEnd]); err != nil {
			return fmt.Errorf("failed to write chunk %d data: %w", chunk.ID, err)
		}
	}

	return nil
}

func (se *StorageEngine) readAndProcessChunkWindow(ctx context.Context, chunk *models.ObjectChunk, shardSize int64) ([]byte, error) {
	shards := se.matrixPool.Get().([][]byte)
	defer func() {
		for i := range shards {
			shards[i] = shards[i][:cap(shards[i])]
		}
		se.matrixPool.Put(shards)
	}()

	recovered := make([]bool, se.TotalShards)
	var wg sync.WaitGroup

	for _, shardMeta := range chunk.Shards {
		idx := shardMeta.Index
		if idx < 0 || idx >= se.TotalShards {
			continue
		}

		drive, exists := se.driveMap[shardMeta.DriveId]
		if !exists {
			continue
		}

		wg.Add(1)
		go func(targetIdx int, d Drive, sm *models.ObjectChunkShard) {
			defer wg.Done()

			shards[targetIdx] = shards[targetIdx][:shardSize]
			path := filepath.Join(d.GetPath(), sm.Path)
			_, err := d.Read(ctx, path, shards[targetIdx])
			if err != nil {
				return
			}

			recovered[targetIdx] = true
		}(idx, drive, shardMeta)
	}
	wg.Wait()

	needsReconstruct := false
	for i := 0; i < se.DataShards; i++ {
		if !recovered[i] {
			needsReconstruct = true
			break
		}
	}

	if needsReconstruct {
		for i := 0; i < se.TotalShards; i++ {
			if !recovered[i] {
				shards[i] = nil
			} else {
				shards[i] = shards[i][:shardSize]
			}
		}

		if err := se.Reconstruct(ctx, shards); err != nil {
			return nil, fmt.Errorf("failed to reconstruct chunk %d: %w", chunk.ID, err)
		}

		// Re-verify all required shards are non-nil after recovery
		for i := 0; i < se.DataShards; i++ {
			if shards[i] == nil {
				// TODO: Do we need to send error? Or can we just return zeroes
				// for missing data shards after reconstruction?
				return nil, fmt.Errorf("failed to reconstruct chunk %d", chunk.ID)
			}
		}
	}

	chunkData := make([]byte, chunk.LogicalSize)
	minShardSize := ComputeShardSize(int(chunk.LogicalSize), se.DataShards)

	for i := 0; i < se.DataShards; i++ {
		startOffset := int64(i * minShardSize)
		if startOffset >= chunk.LogicalSize {
			break
		}
		endOffset := min(startOffset+int64(minShardSize), chunk.LogicalSize)
		bytesToCopy := endOffset - startOffset

		copy(chunkData[startOffset:endOffset], shards[i][:bytesToCopy])
	}

	return chunkData, nil
}

func (se *StorageEngine) Reconstruct(ctx context.Context, shards [][]byte) error {
	if err := se.rsEncoder.ReconstructData(shards); err != nil {
		return err
	}

	return nil
}

// ComputeAlignedShardSize computes the optimal shard size for a given minimum data size,
// ensuring 64-byte alignment for performance.
// Formula:
//
//	((minShardSize + (CPUCacheLine - 1)) / CPUCacheLine) * CPUCacheLine
func ComputeAlignedShardSize(shardSize int) int {
	return (shardSize + (CPUCacheLine - 1)) &^ (CPUCacheLine - 1)
}

// ComputeShardSize calculates the minimum shard size using needed to store the given data size
// across the specified number of data shards.
func ComputeShardSize(dataSize, numberOfDataShards int) int {
	return (dataSize + numberOfDataShards - 1) / numberOfDataShards
}

func formatFileName(filename string, chunkId int64, i int) string {
	return fmt.Sprintf("%s.chunk_%d.shard_%d", filename, chunkId, i)
}
