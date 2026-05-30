package xfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/avatar31/dotfs/models"
)

// failDrive wraps a memDrive but always errors on Read, simulating a dead drive.
type failDrive struct {
	*memDrive
}

func (d *failDrive) Read(_ context.Context, path string, _ []byte) (int, error) {
	return 0, fmt.Errorf("drive unavailable: %s", path)
}

// failWriteDrive wraps a memDrive but always errors on Write, simulating a drive that rejects writes.
type failWriteDrive struct {
	*memDrive
}

func (d *failWriteDrive) Write(_ context.Context, path string, _ []byte) error {
	return fmt.Errorf("write failed: disk full: %s", path)
}

var (
	testDataShards        = 4
	testParShards         = 2
	testTotalShards       = testDataShards + testParShards
	fullChunkMinShardSize = ComputeShardSize(MaxChunkSize, testDataShards)
)

func generateDeterministicTestData(size int) []byte {
	// Create repeating A-Z pattern to fill the requested size.
	pattern := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	data := bytes.Repeat(pattern, size/len(pattern)+1)
	return data[:size]
}

func getTestReader(data []byte) io.Reader {
	return bytes.NewReader(data)
}

func getWorkingMemDrives() []Drive {
	drives := make([]Drive, testDataShards+testParShards)
	for i := range drives {
		drives[i] = newMemDrive(int64(i+1), fmt.Sprintf("/dev/memdrive%d", i))
	}
	return drives
}

func getTestShards(filename string, chunkId int64, drives []Drive, shardSize int64) []*models.ObjectChunkShard {
	shards := make([]*models.ObjectChunkShard, testTotalShards)

	for i := range testDataShards {
		shards[i] = &models.ObjectChunkShard{
			Index:        i,
			PhysicalSize: shardSize,
			Path:         filepath.Join("test", formatFileName(filename, chunkId, i)),
			DriveId:      drives[i].GetID(),
			Type:         models.ShardTypeData,
		}
	}
	for i := testDataShards; i < testTotalShards; i++ {
		shards[i] = &models.ObjectChunkShard{
			Index:        i,
			PhysicalSize: shardSize,
			Path:         filepath.Join("test", formatFileName(filename, chunkId, i)),
			DriveId:      drives[i].GetID(),
			Type:         models.ShardTypeParity,
		}
	}
	return shards
}

func getChunksFor10MBFile(filename string, drives []Drive) []*models.ObjectChunk {
	remainingAfterTwoFullChunks := int64((10 * MegaByte) - (MaxChunkSize * 2)) // 2 MB

	chunk1 := &models.ObjectChunk{
		ID:           1,
		Start:        0,
		End:          MaxChunkSize - 1,
		LogicalSize:  MaxChunkSize, // 4 MB
		PhysicalSize: int64(fullChunkMinShardSize * testTotalShards),
		Shards:       getTestShards(filename, 1, drives, int64(fullChunkMinShardSize)),
	}
	chunk2 := &models.ObjectChunk{
		ID:           2,
		Start:        MaxChunkSize,
		End:          (MaxChunkSize * 2) - 1,
		LogicalSize:  MaxChunkSize, // 4 MB
		PhysicalSize: int64(fullChunkMinShardSize * testTotalShards),
		Shards:       getTestShards(filename, 2, drives, int64(fullChunkMinShardSize)),
	}
	chunk3 := &models.ObjectChunk{
		ID:           3,
		Start:        MaxChunkSize * 2,
		End:          (10 * MegaByte) - 1,
		LogicalSize:  remainingAfterTwoFullChunks, // 2 MB
		PhysicalSize: int64((ComputeShardSize(int(remainingAfterTwoFullChunks), testDataShards)) * testTotalShards),
		Shards:       getTestShards(filename, 3, drives, int64(ComputeShardSize(int(remainingAfterTwoFullChunks), testDataShards))),
	}

	return []*models.ObjectChunk{chunk1, chunk2, chunk3}
}

func TestReadWriteSuccess(t *testing.T) {
	workingDrives := getWorkingMemDrives()
	TenMBData := generateDeterministicTestData(10 * MegaByte)

	test := []struct {
		name                 string
		drives               []Drive
		objectMeta           *models.ObjectStorageMeta
		inputData            io.Reader
		expectedObjectChunks []*models.ObjectChunk
		readOffset           int64
		readSize             int64
		expectedReadData     string
	}{
		{
			name:       "File with minimal size (1 byte) with full read",
			drives:     workingDrives,
			objectMeta: &models.ObjectStorageMeta{ObjectId: "1", Size: 1, Filename: "1byte.txt", Path: "test"},
			inputData:  getTestReader(generateDeterministicTestData(1)),
			expectedObjectChunks: func() []*models.ObjectChunk {
				return []*models.ObjectChunk{{
					ID:           1,
					Start:        0,
					End:          0,
					LogicalSize:  1,
					PhysicalSize: int64(CPUCacheLine * testTotalShards), // 1 byte data + padding to fill all data shards
					Shards:       getTestShards("1byte.txt", 1, workingDrives, CPUCacheLine),
				}}
			}(),
			readOffset:       0,
			readSize:         1,
			expectedReadData: string(generateDeterministicTestData(1)),
		},
		{
			name:       "File with 10 byte size with full read",
			drives:     workingDrives,
			objectMeta: &models.ObjectStorageMeta{ObjectId: "1", Size: 10, Filename: "10byte.txt", Path: "test"},
			inputData:  getTestReader(generateDeterministicTestData(10)),
			expectedObjectChunks: func() []*models.ObjectChunk {
				return []*models.ObjectChunk{{
					ID:           1,
					Start:        0,
					End:          9,
					LogicalSize:  10,
					PhysicalSize: int64(CPUCacheLine * testTotalShards), // 10 byte data + padding to fill all data shards
					Shards:       getTestShards("10byte.txt", 1, workingDrives, CPUCacheLine),
				}}
			}(),
			readOffset:       0,
			readSize:         10,
			expectedReadData: string(generateDeterministicTestData(10)),
		},
		{
			name:       "File with size exactly equal to one chunk (4 MB) with full read",
			drives:     workingDrives,
			objectMeta: &models.ObjectStorageMeta{ObjectId: "2", Size: MaxChunkSize, Filename: "4mb.txt", Path: "test"},
			inputData:  getTestReader(generateDeterministicTestData(MaxChunkSize)),
			expectedObjectChunks: func() []*models.ObjectChunk {
				return []*models.ObjectChunk{{
					ID:           1,
					Start:        0,
					End:          MaxChunkSize - 1,
					LogicalSize:  MaxChunkSize,
					PhysicalSize: int64((fullChunkMinShardSize) * testTotalShards),
					Shards:       getTestShards("4mb.txt", 1, workingDrives, int64(fullChunkMinShardSize)),
				}}
			}(),
			readOffset:       0,
			readSize:         MaxChunkSize,
			expectedReadData: string(generateDeterministicTestData(MaxChunkSize)),
		},
		{
			name:       "File with size just above one chunk (4 MB + 1 byte) with full read",
			drives:     workingDrives,
			objectMeta: &models.ObjectStorageMeta{ObjectId: "5", Size: MaxChunkSize + 1, Filename: "4mb_plus_1.txt", Path: "test"},
			inputData:  getTestReader(generateDeterministicTestData(MaxChunkSize + 1)),
			expectedObjectChunks: []*models.ObjectChunk{
				{
					ID:           1,
					Start:        0,
					End:          MaxChunkSize - 1,
					LogicalSize:  MaxChunkSize,
					PhysicalSize: int64(fullChunkMinShardSize * testTotalShards),
					Shards:       getTestShards("4mb_plus_1.txt", 1, workingDrives, int64(fullChunkMinShardSize)),
				},
				{
					ID:           2,
					Start:        MaxChunkSize,
					End:          MaxChunkSize,
					LogicalSize:  1,
					PhysicalSize: int64(CPUCacheLine * testTotalShards),
					Shards:       getTestShards("4mb_plus_1.txt", 2, workingDrives, CPUCacheLine),
				},
			},
			readOffset:       0,
			readSize:         MaxChunkSize + 1,
			expectedReadData: string(generateDeterministicTestData(MaxChunkSize + 1)),
		},
		{
			name:                 "File with size spanning multiple chunks (10 MB) with full read",
			drives:               workingDrives,
			objectMeta:           &models.ObjectStorageMeta{ObjectId: "3", Size: 10 * MegaByte, Filename: "10mb.txt", Path: "test"},
			inputData:            getTestReader(TenMBData),
			expectedObjectChunks: getChunksFor10MBFile("10mb.txt", workingDrives),
			readOffset:           0,
			readSize:             10 * MegaByte,
			expectedReadData:     string(TenMBData),
		},
		{
			name:                 "File with size spanning multiple chunks (10 MB) with partial read by reading first chunk (0..4MB)",
			drives:               workingDrives,
			objectMeta:           &models.ObjectStorageMeta{ObjectId: "3", Size: 10 * MegaByte, Filename: "10mb.txt", Path: "test"},
			inputData:            getTestReader(TenMBData),
			expectedObjectChunks: getChunksFor10MBFile("10mb.txt", workingDrives),
			readOffset:           0,
			readSize:             MaxChunkSize,
			expectedReadData:     string(generateDeterministicTestData(MaxChunkSize)),
		},
		{
			name:                 "File with size spanning multiple chunks (10 MB) with partial read by reading middle chunk (4MB..8MB)",
			drives:               workingDrives,
			objectMeta:           &models.ObjectStorageMeta{ObjectId: "3", Size: 10 * MegaByte, Filename: "10mb.txt", Path: "test"},
			inputData:            getTestReader(TenMBData),
			expectedObjectChunks: getChunksFor10MBFile("10mb.txt", workingDrives),
			readOffset:           MaxChunkSize,
			readSize:             MaxChunkSize,
			expectedReadData:     string(TenMBData[MaxChunkSize : MaxChunkSize+MaxChunkSize]),
		},
		{
			name:                 "File with size spanning multiple chunks (10 MB) with partial read by reading last chunk (8MB..10MB)",
			drives:               workingDrives,
			objectMeta:           &models.ObjectStorageMeta{ObjectId: "3", Size: 10 * MegaByte, Filename: "10mb.txt", Path: "test"},
			inputData:            getTestReader(TenMBData),
			expectedObjectChunks: getChunksFor10MBFile("10mb.txt", workingDrives),
			readOffset:           MaxChunkSize * 2,
			readSize:             2 * MegaByte,
			expectedReadData:     string(TenMBData[MaxChunkSize*2 : (MaxChunkSize*2)+(2*MegaByte)]),
		},
		{
			name:                 "File with size spanning multiple chunks (10 MB) with Cross-chunk read spanning chunks 1 and 2 (3MB..6MB)",
			drives:               workingDrives,
			objectMeta:           &models.ObjectStorageMeta{ObjectId: "3", Size: 10 * MegaByte, Filename: "10mb.txt", Path: "test"},
			inputData:            getTestReader(TenMBData),
			expectedObjectChunks: getChunksFor10MBFile("10mb.txt", workingDrives),
			readOffset:           3 * MegaByte,
			readSize:             3 * MegaByte,
			expectedReadData:     string(TenMBData[3*MegaByte : (3*MegaByte)+(3*MegaByte)]),
		},
		{
			name:                 "File with size spanning multiple chunks (10 MB) with Cross-chunk read spanning chunks 2 and 3 (6MB..10MB)",
			drives:               workingDrives,
			objectMeta:           &models.ObjectStorageMeta{ObjectId: "3", Size: 10 * MegaByte, Filename: "10mb.txt", Path: "test"},
			inputData:            getTestReader(TenMBData),
			expectedObjectChunks: getChunksFor10MBFile("10mb.txt", workingDrives),
			readOffset:           6 * MegaByte,
			readSize:             4 * MegaByte,
			expectedReadData:     string(TenMBData[6*MegaByte : (6*MegaByte)+(4*MegaByte)]),
		},
		{
			name:                 "File with size spanning multiple chunks (10 MB) with Cross-chunk read spanning all three chunks (3MB..8MB)",
			drives:               workingDrives,
			objectMeta:           &models.ObjectStorageMeta{ObjectId: "3", Size: 10 * MegaByte, Filename: "10mb.txt", Path: "test"},
			inputData:            getTestReader(TenMBData),
			expectedObjectChunks: getChunksFor10MBFile("10mb.txt", workingDrives),
			readOffset:           3 * MegaByte,
			readSize:             5 * MegaByte,
			expectedReadData:     string(TenMBData[3*MegaByte : (3*MegaByte)+(5*MegaByte)]),
		},
		{
			name:                 "File with size spanning multiple chunks (10 MB) with Single byte read at chunk boundary (byte 4MB)",
			drives:               workingDrives,
			objectMeta:           &models.ObjectStorageMeta{ObjectId: "3", Size: 10 * MegaByte, Filename: "10mb.txt", Path: "test"},
			inputData:            getTestReader(TenMBData),
			expectedObjectChunks: getChunksFor10MBFile("10mb.txt", workingDrives),
			readOffset:           MaxChunkSize,
			readSize:             1,
			expectedReadData:     string(TenMBData[MaxChunkSize : MaxChunkSize+1]),
		},
		{
			name:                 "File with size spanning multiple chunks (10 MB) with Single byte read at end of first chunk (byte 4MB-1)",
			drives:               workingDrives,
			objectMeta:           &models.ObjectStorageMeta{ObjectId: "3", Size: 10 * MegaByte, Filename: "10mb.txt", Path: "test"},
			inputData:            getTestReader(TenMBData),
			expectedObjectChunks: getChunksFor10MBFile("10mb.txt", workingDrives),
			readOffset:           MaxChunkSize - 1,
			readSize:             1,
			expectedReadData:     string(TenMBData[MaxChunkSize-1 : MaxChunkSize]),
		},
	}

	for _, tc := range test {
		t.Run(tc.name, func(t *testing.T) {
			engine, err := NewStorageEngine(testDataShards, testParShards, tc.drives)
			assert.NoError(t, err)

			chunks, err := engine.EncodeObjectByChunks(context.Background(), tc.objectMeta, tc.inputData)
			assert.NoError(t, err)

			assert.Len(t, chunks, len(tc.expectedObjectChunks))
			for i := range chunks {
				assert.Equal(t, tc.expectedObjectChunks[i].ID, chunks[i].ID)
				assert.Equal(t, tc.expectedObjectChunks[i].Start, chunks[i].Start)
				assert.Equal(t, tc.expectedObjectChunks[i].End, chunks[i].End)
				assert.Equal(t, tc.expectedObjectChunks[i].LogicalSize, chunks[i].LogicalSize)
				assert.Equal(t, tc.expectedObjectChunks[i].PhysicalSize, chunks[i].PhysicalSize)

				assert.Len(t, chunks[i].Shards, testTotalShards)
				for j := range chunks[i].Shards {
					assert.Equal(t, tc.expectedObjectChunks[i].Shards[j].Index, chunks[i].Shards[j].Index)
					assert.Equal(t, tc.expectedObjectChunks[i].Shards[j].PhysicalSize, chunks[i].Shards[j].PhysicalSize)
					assert.Equal(t, tc.expectedObjectChunks[i].Shards[j].Path, chunks[i].Shards[j].Path)
					assert.Equal(t, tc.expectedObjectChunks[i].Shards[j].DriveId, chunks[i].Shards[j].DriveId)
					assert.Equal(t, tc.expectedObjectChunks[i].Shards[j].Type, chunks[i].Shards[j].Type)
				}
			}

			tc.objectMeta.Chunks = chunks
			buf := new(bytes.Buffer)
			err = engine.ReadObjectByRange(context.Background(), tc.objectMeta, tc.readOffset, tc.readSize, buf)
			assert.NoError(t, err)

			assert.Equal(t, tc.readSize, int64(buf.Len()), "Read size didn't match expected size. Expected: %d, Got: %d",
				tc.readSize, buf.Len())
			assert.Equal(t, tc.expectedReadData, buf.String(), "Read didn't match expected data")
		})
	}
}

func TestDriveFailureRecovery(t *testing.T) {
	const fileSize = MaxChunkSize
	inputData := generateDeterministicTestData(fileSize)

	tests := []struct {
		name            string
		failedDriveIdxs []int
		expectError     bool
	}{
		{
			name:            "1 data drive fails - within parity tolerance",
			failedDriveIdxs: []int{0},
			expectError:     false,
		},
		{
			name:            "2 drives fail - at parity limit",
			failedDriveIdxs: []int{0, 1},
			expectError:     false,
		},
		{
			name:            "3 drives fail - exceeds parity tolerance",
			failedDriveIdxs: []int{0, 1, 2},
			expectError:     true,
		},
		{
			name:            "2 parity drives fail - data shards intact",
			failedDriveIdxs: []int{4, 5},
			expectError:     false,
		},
		{
			name:            "1 data shard and 1 parity shard fail - within tolerance",
			failedDriveIdxs: []int{2, 4},
			expectError:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Step 1: Write using all working drives
			baseDrives := make([]*memDrive, testTotalShards)
			writeDrives := make([]Drive, testTotalShards)
			for i := range baseDrives {
				baseDrives[i] = newMemDrive(int64(i+1), fmt.Sprintf("/dev/memdrive%d", i))
				writeDrives[i] = baseDrives[i]
			}

			writeEngine, err := NewStorageEngine(testDataShards, testParShards, writeDrives)
			assert.NoError(t, err)

			meta := &models.ObjectStorageMeta{
				ObjectId: "recovery-test",
				Size:     fileSize,
				Filename: "recovery.txt",
				Path:     "test",
			}

			chunks, err := writeEngine.EncodeObjectByChunks(context.Background(), meta, getTestReader(inputData))
			assert.NoError(t, err)
			meta.Chunks = chunks

			// Step 2: Build read engine with some drives substituted as failDrives
			readDrives := make([]Drive, testTotalShards)
			for i := range baseDrives {
				readDrives[i] = baseDrives[i]
			}
			for _, idx := range tc.failedDriveIdxs {
				readDrives[idx] = &failDrive{baseDrives[idx]}
			}

			readEngine, err := NewStorageEngine(testDataShards, testParShards, readDrives)
			assert.NoError(t, err)

			buf := new(bytes.Buffer)
			err = readEngine.ReadObjectByRange(context.Background(), meta, 0, int64(fileSize), buf)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, inputData, buf.Bytes())
			}
		})
	}
}

func TestErrorConditions(t *testing.T) {
	t.Run("NewStorageEngine rejects insufficient drives", func(t *testing.T) {
		drives := make([]Drive, testTotalShards-1)
		for i := range drives {
			drives[i] = newMemDrive(int64(i+1), fmt.Sprintf("/dev/memdrive%d", i))
		}
		_, err := NewStorageEngine(testDataShards, testParShards, drives)
		assert.Error(t, err)
	})

	t.Run("EncodeObjectByChunks fails on drive write error", func(t *testing.T) {
		drives := make([]Drive, testTotalShards)
		for i := range drives {
			base := newMemDrive(int64(i+1), fmt.Sprintf("/dev/memdrive%d", i))
			if i == 0 {
				drives[i] = &failWriteDrive{base}
			} else {
				drives[i] = base
			}
		}
		engine, err := NewStorageEngine(testDataShards, testParShards, drives)
		assert.NoError(t, err)

		meta := &models.ObjectStorageMeta{
			ObjectId: "err-write",
			Size:     10,
			Filename: "err.txt",
			Path:     "test",
		}
		_, err = engine.EncodeObjectByChunks(
			context.Background(), meta, getTestReader(generateDeterministicTestData(10)),
		)
		assert.Error(t, err)
	})

	t.Run("EncodeObjectByChunks respects context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		engine, err := NewStorageEngine(testDataShards, testParShards, getWorkingMemDrives())
		assert.NoError(t, err)

		meta := &models.ObjectStorageMeta{
			ObjectId: "cancel-write",
			Size:     MaxChunkSize * 3,
			Filename: "cancel.txt",
			Path:     "test",
		}
		_, err = engine.EncodeObjectByChunks(
			ctx, meta, getTestReader(generateDeterministicTestData(MaxChunkSize*3)),
		)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("ReadObjectByRange respects context cancellation", func(t *testing.T) {
		drives := getWorkingMemDrives()
		engine, err := NewStorageEngine(testDataShards, testParShards, drives)
		assert.NoError(t, err)

		meta := &models.ObjectStorageMeta{
			ObjectId: "cancel-read",
			Size:     MaxChunkSize,
			Filename: "cancel_read.txt",
			Path:     "test",
		}
		chunks, err := engine.EncodeObjectByChunks(
			context.Background(), meta, getTestReader(generateDeterministicTestData(MaxChunkSize)),
		)
		assert.NoError(t, err)
		meta.Chunks = chunks

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		buf := new(bytes.Buffer)
		err = engine.ReadObjectByRange(ctx, meta, 0, MaxChunkSize, buf)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

// ************* BENCHMARKS *************

// Benchmark Test Run Date: 30/05/2026
// StorageEngine.EncodeObjectByChunks with varying input sizes, both sequential and parallel execution
//
// goos: linux
// goarch: amd64
// pkg: github.com/avatar31/dotfs/fsal/xfs
// cpu: Intel(R) Xeon(R) CPU E5-2698 v4 @ 2.20GHz
// BenchmarkEncodeObjectByChunks/___1_Byte_Sequential-32            3964112               262.4 ns/op            35 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/_10_Bytes_Sequential-32            5049697               277.5 ns/op            32 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/100_Bytes_Sequential-32            3832717               302.3 ns/op            34 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/_____1_KB_Sequential-32            3890738               283.2 ns/op            34 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/____10_KB_Sequential-32            4382577               282.6 ns/op            34 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/___100_KB_Sequential-32            3977504               281.5 ns/op            33 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/_____1_MB_Sequential-32            4075522               256.0 ns/op            35 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/_____4_MB_Sequential-32            3996378               256.9 ns/op            33 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/____10_MB_Sequential-32            5247495               239.5 ns/op            32 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/___1_Byte___Parallel-32           95389552                12.05 ns/op           32 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/_10_Bytes___Parallel-32           87579350                13.40 ns/op           32 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/100_Bytes___Parallel-32           104936760               12.27 ns/op           32 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/_____1_KB___Parallel-32           99328374                12.35 ns/op           32 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/____10_KB___Parallel-32           85454770                12.71 ns/op           32 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/___100_KB___Parallel-32           96725962                13.60 ns/op           32 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/_____1_MB___Parallel-32           97090353                12.98 ns/op           32 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/_____4_MB___Parallel-32           99338318                13.79 ns/op           32 B/op          2 allocs/op
// BenchmarkEncodeObjectByChunks/____10_MB___Parallel-32           85012278                12.96 ns/op           32 B/op          2 allocs/op
// PASS
// ok      github.com/avatar31/dotfs/fsal/xfs      34.744s
func BenchmarkEncodeObjectByChunks(b *testing.B) {
	engine, err := NewStorageEngine(testDataShards, testParShards, getWorkingMemDrives())
	assert.NoError(b, err)

	meta := &models.ObjectStorageMeta{
		ObjectId: "benchmark",
		Filename: "benchmark.txt",
		Path:     "test",
	}

	benchmarks := []struct {
		name string
		data io.Reader
	}{
		{
			name: "___1 Byte",
			data: getTestReader(generateDeterministicTestData(1)),
		},
		{
			name: "_10 Bytes",
			data: getTestReader(generateDeterministicTestData(10)),
		},
		{
			name: "100 Bytes",
			data: getTestReader(generateDeterministicTestData(100)),
		},
		{
			name: "_____1 KB",
			data: getTestReader(generateDeterministicTestData(1 * KiloByte)),
		},
		{
			name: "____10 KB",
			data: getTestReader(generateDeterministicTestData(10 * KiloByte)),
		},
		{
			name: "___100 KB",
			data: getTestReader(generateDeterministicTestData(100 * KiloByte)),
		},
		{
			name: "_____1 MB",
			data: getTestReader(generateDeterministicTestData(1 * MegaByte)),
		},
		{
			name: "_____4 MB",
			data: getTestReader(generateDeterministicTestData(4 * MegaByte)),
		},
		{
			name: "____10 MB",
			data: getTestReader(generateDeterministicTestData(10 * MegaByte)),
		},
	}

	b.ResetTimer()
	for _, bm := range benchmarks {
		b.Run(bm.name+"_Sequential", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = engine.EncodeObjectByChunks(context.Background(), meta, bm.data)
			}
		})
	}

	for _, bm := range benchmarks {
		b.Run(bm.name+"___Parallel", func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, _ = engine.EncodeObjectByChunks(context.Background(), meta, bm.data)
				}
			})
		})
	}
}

// Benchmark Test Run Date: 30/05/2026
// StorageEngine.ReadObjectByRange with varying input sizes without reconstructing, both sequential and parallel execution
//
// goos: linux
// goarch: amd64
// pkg: github.com/avatar31/dotfs/fsal/xfs
// cpu: Intel(R) Xeon(R) CPU E5-2698 v4 @ 2.20GHz
// BenchmarkReadObjectByRange/___1_Byte_Full_Read_Sequential-32               54993             19045 ns/op            1638 B/op         24 allocs/op
// BenchmarkReadObjectByRange/_10_Bytes_Full_Read_Sequential-32               54595             18652 ns/op            1531 B/op         24 allocs/op
// BenchmarkReadObjectByRange/100_Bytes_Full_Read_Sequential-32               50236             20470 ns/op            1693 B/op         24 allocs/op
// BenchmarkReadObjectByRange/_____1_KB_Full_Read_Sequential-32               36366             28983 ns/op            3565 B/op         24 allocs/op
// BenchmarkReadObjectByRange/____10_KB_Full_Read_Sequential-32               28629             38791 ns/op           22486 B/op         24 allocs/op
// BenchmarkReadObjectByRange/___100_KB_Full_Read_Sequential-32                4624            224119 ns/op          238863 B/op         24 allocs/op
// BenchmarkReadObjectByRange/_____1_MB_Full_Read_Sequential-32                 640           1717654 ns/op         2285617 B/op         24 allocs/op
// BenchmarkReadObjectByRange/_____4_MB_Full_Read_Sequential-32                 136           8282536 ns/op         8437375 B/op         24 allocs/op
// BenchmarkReadObjectByRange/____10_MB_Full_Read_Sequential-32                  22          47968836 ns/op        42427708 B/op         75 allocs/op
// BenchmarkReadObjectByRange/___1_Byte_Full_Read___Parallel-32            51010140                24.48 ns/op           48 B/op          1 allocs/op
// BenchmarkReadObjectByRange/_10_Bytes_Full_Read___Parallel-32            56381806                24.52 ns/op           48 B/op          1 allocs/op
// BenchmarkReadObjectByRange/100_Bytes_Full_Read___Parallel-32            64065188                23.35 ns/op           48 B/op          1 allocs/op
// BenchmarkReadObjectByRange/_____1_KB_Full_Read___Parallel-32            59036188                24.50 ns/op           48 B/op          1 allocs/op
// BenchmarkReadObjectByRange/____10_KB_Full_Read___Parallel-32            58875752                23.76 ns/op           48 B/op          1 allocs/op
// BenchmarkReadObjectByRange/___100_KB_Full_Read___Parallel-32            85560050                23.43 ns/op           48 B/op          1 allocs/op
// BenchmarkReadObjectByRange/_____1_MB_Full_Read___Parallel-32            57872354                25.11 ns/op           48 B/op          1 allocs/op
// BenchmarkReadObjectByRange/_____4_MB_Full_Read___Parallel-32            73829329                23.80 ns/op           48 B/op          1 allocs/op
// BenchmarkReadObjectByRange/____10_MB_Full_Read___Parallel-32            62655414                23.48 ns/op           48 B/op          1 allocs/op
// PASS
// ok      github.com/avatar31/dotfs/fsal/xfs      29.007s
func BenchmarkReadObjectByRange(b *testing.B) {
	engine, err := NewStorageEngine(testDataShards, testParShards, getWorkingMemDrives())
	assert.NoError(b, err)

	meta1BObj := &models.ObjectStorageMeta{ObjectId: "benchmark_1b", Filename: "benchmark_1b.txt", Path: "test"}
	chunks1B, err := engine.EncodeObjectByChunks(context.Background(), meta1BObj, getTestReader(generateDeterministicTestData(1)))
	assert.NoError(b, err)
	meta1BObj.Chunks = chunks1B

	meta10BObj := &models.ObjectStorageMeta{ObjectId: "benchmark_10b", Filename: "benchmark_10b.txt", Path: "test"}
	chunks10B, err := engine.EncodeObjectByChunks(context.Background(), meta10BObj, getTestReader(generateDeterministicTestData(10)))
	assert.NoError(b, err)
	meta10BObj.Chunks = chunks10B

	meta100BObj := &models.ObjectStorageMeta{ObjectId: "benchmark_100b", Filename: "benchmark_100b.txt", Path: "test"}
	chunks100B, err := engine.EncodeObjectByChunks(context.Background(), meta100BObj, getTestReader(generateDeterministicTestData(100)))
	assert.NoError(b, err)
	meta100BObj.Chunks = chunks100B

	meta1KBObj := &models.ObjectStorageMeta{ObjectId: "benchmark_1kb", Filename: "benchmark_1kb.txt", Path: "test"}
	chunks1KB, err := engine.EncodeObjectByChunks(context.Background(), meta1KBObj, getTestReader(generateDeterministicTestData(1*KiloByte)))
	assert.NoError(b, err)
	meta1KBObj.Chunks = chunks1KB

	meta10KBObj := &models.ObjectStorageMeta{ObjectId: "benchmark_10kb", Filename: "benchmark_10kb.txt", Path: "test"}
	chunks10KB, err := engine.EncodeObjectByChunks(context.Background(), meta10KBObj, getTestReader(generateDeterministicTestData(10*KiloByte)))
	assert.NoError(b, err)
	meta10KBObj.Chunks = chunks10KB

	meta100KBObj := &models.ObjectStorageMeta{ObjectId: "benchmark_100kb", Filename: "benchmark_100kb.txt", Path: "test"}
	chunks100KB, err := engine.EncodeObjectByChunks(context.Background(), meta100KBObj, getTestReader(generateDeterministicTestData(100*KiloByte)))
	assert.NoError(b, err)
	meta100KBObj.Chunks = chunks100KB

	meta1MBObj := &models.ObjectStorageMeta{ObjectId: "benchmark_1mb", Filename: "benchmark_1mb.txt", Path: "test"}
	chunks1MB, err := engine.EncodeObjectByChunks(context.Background(), meta1MBObj, getTestReader(generateDeterministicTestData(1*MegaByte)))
	assert.NoError(b, err)
	meta1MBObj.Chunks = chunks1MB

	meta4MBObj := &models.ObjectStorageMeta{ObjectId: "benchmark_4mb", Filename: "benchmark_4mb.txt", Path: "test"}
	chunks4MB, err := engine.EncodeObjectByChunks(context.Background(), meta4MBObj, getTestReader(generateDeterministicTestData(4*MegaByte)))
	assert.NoError(b, err)
	meta4MBObj.Chunks = chunks4MB

	meta10MBObj := &models.ObjectStorageMeta{ObjectId: "benchmark_10mb", Filename: "benchmark_10mb.txt", Path: "test"}
	chunks10MB, err := engine.EncodeObjectByChunks(context.Background(), meta10MBObj, getTestReader(generateDeterministicTestData(10*MegaByte)))
	assert.NoError(b, err)
	meta10MBObj.Chunks = chunks10MB

	benchmarks := []struct {
		name       string
		objectMeta *models.ObjectStorageMeta
		readSize   int64
	}{
		{
			name:       "___1 Byte Full Read",
			objectMeta: meta1BObj,
			readSize:   1,
		},
		{
			name:       "_10 Bytes Full Read",
			objectMeta: meta10BObj,
			readSize:   10,
		},
		{
			name:       "100 Bytes Full Read",
			objectMeta: meta100BObj,
			readSize:   100,
		},
		{
			name:       "_____1 KB Full Read",
			objectMeta: meta1KBObj,
			readSize:   1 * KiloByte,
		},
		{
			name:       "____10 KB Full Read",
			objectMeta: meta10KBObj,
			readSize:   10 * KiloByte,
		},
		{
			name:       "___100 KB Full Read",
			objectMeta: meta100KBObj,
			readSize:   100 * KiloByte,
		},
		{
			name:       "_____1 MB Full Read",
			objectMeta: meta1MBObj,
			readSize:   1 * MegaByte,
		},
		{
			name:       "_____4 MB Full Read",
			objectMeta: meta4MBObj,
			readSize:   4 * MegaByte,
		},
		{
			name:       "____10 MB Full Read",
			objectMeta: meta10MBObj,
			readSize:   10 * MegaByte,
		},
	}

	b.ResetTimer()
	for _, bm := range benchmarks {
		b.Run(bm.name+"_Sequential", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				buf := new(bytes.Buffer)
				_ = engine.ReadObjectByRange(context.Background(), bm.objectMeta, 0, bm.readSize, buf)
			}
		})
	}

	for _, bm := range benchmarks {
		b.Run(bm.name+"___Parallel", func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					buf := new(bytes.Buffer)
					_ = engine.ReadObjectByRange(context.Background(), bm.objectMeta, 0, bm.objectMeta.Size, buf)
				}
			})
		})
	}
}
