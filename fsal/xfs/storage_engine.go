package xfs

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"
)

const (
	MaxChunkSize = 4 * 1024 * 1024 // 4 MB fixed chunk size
)

type StorageEngine struct {
	drives []Drive

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

	maxShardSize := (MaxChunkSize + dataShards - 1) / dataShards

	return &StorageEngine{
		drives:       drives,
		rsEncoder:    enc,
		DataShards:   dataShards,
		ParityShards: parityShards,
		TotalShards:  totalShards,
		matrixPool: &sync.Pool{
			New: func() any {
				matrix := make([][]byte, totalShards)
				for i := range matrix {
					matrix[i] = make([]byte, maxShardSize)
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
func (se *StorageEngine) EncodeObjectByChunks(ctx context.Context, objectId string, size int64, data io.Reader) error {
	// A reusable buffer to swallow up to 4MB from the reader at a time
	dataBuffer := se.chunkPool.Get().([]byte)
	defer se.chunkPool.Put(dataBuffer)

	var chunkIdx int64 = 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		bytesRead, err := io.ReadFull(data, dataBuffer)
		if err == io.EOF || (err == io.ErrUnexpectedEOF && bytesRead == 0) {
			break
		}

		if err != nil && err != io.ErrUnexpectedEOF {
			return fmt.Errorf("failed to read chunk %d from stream: %w", chunkIdx, err)
		}

		err = se.EncodeSingleChunk(ctx, objectId, chunkIdx, dataBuffer[:bytesRead])
		if err != nil {
			return err
		}

		chunkIdx++
	}

	return nil
}

func (se *StorageEngine) EncodeSingleChunk(ctx context.Context, objectId string, chunkId int64, chunkData []byte) error {
	shards := se.matrixPool.Get().([][]byte)
	defer se.matrixPool.Put(shards)

	// Dynamic calculation of shard sizes (e.g., 2560 bytes for a 10KB file)
	currentShardSize := (len(chunkData) + se.DataShards - 1) / se.DataShards
	for i := range shards {
		clear(shards[i][:currentShardSize])
	}

	// Pivot sequential data into parallel shard structures
	for i := 0; i < se.DataShards; i++ {
		start := i * currentShardSize
		if start >= len(chunkData) {
			break
		}
		end := min(start+currentShardSize, len(chunkData))
		copy(shards[i][:end-start], chunkData[start:end])
	}

	err := se.rsEncoder.Encode(shards[:se.TotalShards])
	if err != nil {
		return fmt.Errorf("erasure coding computation failed: %w", err)
	}

	err = se.dispatchChunkToPhysicalStorage(ctx, objectId, chunkId, shards, currentShardSize)
	if err != nil {
		return fmt.Errorf("chunk dispatch failed: %w", err)
	}

	// TODO: Atomic Commits: Store the chunk shards inside a temporary path (e.g., .tmp/) first.
	// Once all shards return a successful 200 OK or io.EOF, write a small metadata manifest
	// record changing its status to COMMITTED. If an operation fails, the index layer simply
	// ignores uncommitted blocks, and a scavenger sweeps .tmp/ periodically.

	return nil
}

func (se *StorageEngine) dispatchChunkToPhysicalStorage(ctx context.Context, objectId string, chunkId int64, shards [][]byte, currentShardSize int) error {
	gCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errChan := make(chan error, se.TotalShards)

	for i := 0; i < se.TotalShards; i++ {
		if i >= len(se.drives) {
			return fmt.Errorf("insufficient physical drives mapped to complete erasure layout allocation")
		}

		wg.Add(1)
		go func(shardIdx int, drive Drive) {
			defer wg.Done()

			if gCtx.Err() != nil {
				return
			}

			fullpath := fmt.Sprintf("%s/%s.chunk_%d.shard_%d", drive.GetPath(), objectId, chunkId, shardIdx)
			err := drive.Write(gCtx, fullpath, shards[shardIdx][:currentShardSize])
			if err != nil {
				fmt.Printf("Error writing shard %d to drive %s: %v\n", shardIdx, drive.GetPath(), err)
				errChan <- fmt.Errorf("failed to write shard %d: %w", shardIdx, err)
				cancel()
				return
			}
		}(i, se.drives[i])
	}

	wg.Wait()
	close(errChan)

	if len(errChan) > 0 {
		se.triggerRollbackCleanup(objectId, chunkId)
		return <-errChan
	}

	return nil
}

func (se *StorageEngine) triggerRollbackCleanup(objectId string, chunkId int64) {
	// Placeholder for rollback logic to delete any partially written shards in case of failure.
	// This would involve iterating over the drives and removing any shards associated with the failed chunk.
}
