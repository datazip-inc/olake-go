package arrowwriter

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg/internal"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/types"
)

// committedFile is a file an earlier sync committed, holding row 0.
const committedFile = "s3://b/t/data/committed.parquet"

// committedIndex is the index as the last sync committed it.
type committedIndex struct{ types.StreamIndex }

func (committedIndex) Lookup(id string) (types.RowLocation, bool, error) {
	if id == "0" {
		return types.RowLocation{FilePath: committedFile}, true, nil
	}
	return types.RowLocation{}, false, nil
}
func (committedIndex) LastCommittedSnapshot() (int64, error)         { return 0, nil }
func (committedIndex) Commit(*types.StreamIndexThread, *int64) error { return nil }

// uploadServer stands in for the Java server: it hands out file paths and keeps the files uploaded.
type uploadServer struct {
	paths    int
	uploaded map[string][]byte
}

func (s *uploadServer) SendClientRequest(_ context.Context, req any) (any, error) {
	payload := req.(*proto.ArrowPayload)
	switch payload.GetType() {
	case proto.ArrowPayload_JSONSCHEMA:
		return &proto.ArrowIngestResponse{IcebergSchemas: map[string]string{
			fileTypeData:             `{"fields":[{"id":1,"name":"_olake_id"},{"id":2,"name":"_op_type"},{"id":3,"name":"_olake_timestamp"},{"id":4,"name":"payload"}]}`,
			fileTypeEqualityDelete:   `{"fields":[{"id":1,"name":"_olake_id"}]}`,
			fileTypePositionalDelete: `{"fields":[]}`,
		}}, nil
	case proto.ArrowPayload_FILEPATH:
		s.paths++
		return &proto.ArrowIngestResponse{Result: fmt.Sprintf("s3://b/t/data/f%d.parquet", s.paths)}, nil
	case proto.ArrowPayload_UPLOAD_FILE:
		up := payload.GetMetadata().GetFileUpload()
		s.uploaded[up.GetFilePath()] = append([]byte(nil), up.GetFileData()...)
		return &proto.ArrowIngestResponse{}, nil
	case proto.ArrowPayload_REGISTER_AND_COMMIT:
		return &proto.ArrowIngestResponse{}, nil
	}
	return nil, fmt.Errorf("unexpected %v", payload.GetType())
}

// newTestWriter returns a writer on a fake server, and its index thread (nil in eq or append mode).
func newTestWriter(t *testing.T, updateType types.UpdateType, upsertMode bool, partitionInfo []internal.PartitionInfo) (*ArrowWriter, *uploadServer, *types.StreamIndexThread) {
	t.Helper()

	server := &uploadServer{uploaded: map[string][]byte{}}
	streamUpdateType := string(updateType)
	stream := &types.ConfiguredStream{Stream: &types.Stream{Name: "t", Namespace: "n"}, StreamMetadata: types.StreamMetadata{UpdateType: &streamUpdateType}}
	var tableIndex types.StreamIndex
	if upsertMode && updateType.NeedsTableIndex(types.Iceberg) {
		tableIndex = committedIndex{}
	}
	index := types.NewStreamIndexThread(tableIndex)
	schema := map[string]string{"_olake_id": "string", "_op_type": "string", "_olake_timestamp": "timestamptz", "payload": "string"}
	w, err := New(context.Background(), &destination.Options{ThreadID: "th", TableIndex: tableIndex}, partitionInfo, schema, stream, server, upsertMode, index)
	require.NoError(t, err)
	return w, server, index
}

func testRecord(id, op, payload string) types.RawRecord {
	return types.RawRecord{Data: map[string]any{"payload": payload}, OlakeColumns: map[string]any{
		constants.OlakeID: id, constants.OpType: op, constants.OlakeTimestamp: time.Now().UTC()}}
}

// parquetRows reads the named columns of an uploaded parquet file as text, one slice per row.
func parquetRows(t *testing.T, data []byte, columns ...string) [][]string {
	t.Helper()

	table, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	defer table.Release()

	rows := make([][]string, table.NumRows())
	for _, name := range columns {
		indices := table.Schema().FieldIndices(name)
		require.Len(t, indices, 1, "column %s", name)
		row := 0
		for _, chunk := range table.Column(indices[0]).Data().Chunks() {
			for idx := range chunk.Len() {
				rows[row] = append(rows[row], chunk.ValueStr(idx))
				row++
			}
		}
	}
	return rows
}

// assertPageIndex checks every column chunk of an uploaded parquet file has the page index Java
// reads single rows by.
func assertPageIndex(t *testing.T, data []byte) {
	t.Helper()

	reader, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	for group := range reader.NumRowGroups() {
		rowGroup := reader.MetaData().RowGroup(group)
		for column := range rowGroup.NumColumns() {
			chunk, err := rowGroup.ColumnChunk(column)
			require.NoError(t, err)
			assert.NotNil(t, chunk.GetOffsetIndexLocation(), "offset index of row group %d column %d", group, column)
			assert.NotNil(t, chunk.GetColumnIndexLocation(), "column index of row group %d column %d", group, column)
		}
	}
}

// Every location the index hands out holds its row's latest version, whether or not a read closed
// the open file between two batches, and the positional deletes point at the versions replaced.
func TestEnsureReadableKeepsPositions(t *testing.T) {
	testCases := []struct {
		name          string
		read          string           // the row a read asks for between the two batches
		wantFiles     int              // data files at Close
		wantPositions map[string]int64 // each row's position in its file at Close
	}{
		{
			name:          "reading a row in the open file closes it, and the next batch starts a new one",
			read:          "1",
			wantFiles:     2,
			wantPositions: map[string]int64{"1": 0, "2": 1, "3": 2, "4": 2},
		},
		{
			name:          "reading a row in a committed file leaves the open file taking the next batch",
			read:          "0",
			wantFiles:     1,
			wantPositions: map[string]int64{"1": 3, "2": 4, "3": 2, "4": 5},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			w, server, index := newTestWriter(t, types.UpdateTypePosition, true, nil)
			require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "c", "a"), testRecord("2", "c", "b"), testRecord("3", "c", "c")}))
			read, found, err := index.Lookup(tc.read)
			require.NoError(t, err)
			require.True(t, found, "row %s is indexed", tc.read)
			require.NoError(t, w.EnsureReadable(ctx, []string{read.FilePath}))
			require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "u", "a2"), testRecord("2", "u", "b2"), testRecord("4", "c", "d")}))

			require.NoError(t, w.Close(ctx, nil))

			var dataFiles, deleteFiles []string
			for _, files := range w.createdFiles {
				for _, data := range files.DataFiles {
					dataFiles = append(dataFiles, data.FilePath)
				}
				for _, deletes := range files.PosDeleteFiles {
					deleteFiles = append(deleteFiles, deletes.FilePath)
				}
			}
			assert.Len(t, dataFiles, tc.wantFiles)
			stored := map[types.RowLocation][]string{} // each data file row's _olake_id and payload
			for _, path := range dataFiles {
				assertPageIndex(t, server.uploaded[path])
				for position, row := range parquetRows(t, server.uploaded[path], constants.OlakeID, "payload") {
					stored[types.RowLocation{FilePath: path, Position: int64(position)}] = row
				}
			}
			for id, payload := range map[string]string{"1": "a2", "2": "b2", "3": "c", "4": "d"} {
				location, found, err := index.Lookup(id)
				require.NoError(t, err)
				require.True(t, found, "row %s is indexed", id)
				assert.Equal(t, tc.wantPositions[id], location.Position, "position of row %s", id)
				assert.Equal(t, []string{id, payload}, stored[location], "row %s at its location", id)
			}
			var deleted [][]string
			for _, path := range deleteFiles {
				for _, row := range parquetRows(t, server.uploaded[path], "file_path", "pos") {
					position, err := strconv.ParseInt(row[1], 10, 64)
					require.NoError(t, err)
					deleted = append(deleted, stored[types.RowLocation{FilePath: row[0], Position: position}])
				}
			}
			assert.ElementsMatch(t, [][]string{{"1", "a"}, {"2", "b"}}, deleted, "the versions rows 1 and 2 replaced")
		})
	}
}

// Only the open files a read names are uploaded: other partitions keep writing to theirs, and a
// file that is not open, or a partition left without one by schema evolution, uploads nothing.
func TestEnsureReadableUploadsNamedOpenFiles(t *testing.T) {
	testCases := []struct {
		name         string
		evolve       bool     // schema evolution uploads every open file before the read
		read         []string // ids of the rows the read asks for
		wantUploaded []string // ids of the rows whose files are uploaded once EnsureReadable returns
	}{
		{
			name:         "only the named partition's file is uploaded",
			read:         []string{"1"},
			wantUploaded: []string{"1"},
		},
		{
			name:         "every named file is uploaded",
			read:         []string{"1", "2"},
			wantUploaded: []string{"1", "2"},
		},
		{
			name: "a row in a file an earlier sync committed uploads nothing",
			read: []string{"0"},
		},
		{
			name:         "a partition schema evolution left without an open file is skipped",
			evolve:       true,
			read:         []string{"1"},
			wantUploaded: []string{"1", "2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			w, server, index := newTestWriter(t, types.UpdateTypePosition, true, []internal.PartitionInfo{{Field: "payload", SchemaField: "payload", Transform: "identity"}})
			require.NoError(t, w.Write(ctx, []types.RawRecord{testRecord("1", "c", "a"), testRecord("2", "c", "b")}))
			filesOf := func(ids []string) []string {
				var paths []string
				for _, id := range ids {
					location, found, err := index.Lookup(id)
					require.NoError(t, err)
					require.True(t, found, "row %s is indexed", id)
					paths = append(paths, location.FilePath)
				}
				return paths
			}
			if tc.evolve {
				require.NoError(t, w.EvolveSchema(ctx, w.schema))
			}

			require.NoError(t, w.EnsureReadable(ctx, filesOf(tc.read)))

			assert.ElementsMatch(t, filesOf(tc.wantUploaded), slices.Collect(maps.Keys(server.uploaded)))
		})
	}
}

// In eq mode a row's first change in a sync gets an equality delete ("c" gets none), and a repeat deletes the previous row by position.
func TestExtractEqModeDeletes(t *testing.T) {
	type change struct{ op, id string }
	const dataFile = "s3://b/t/data/f1.parquet" // the first path the uploadServer hands out

	testCases := []struct {
		name              string
		upsertMode        bool
		ops               []change
		equalityDeletes   []string
		positionalDeletes []PositionalDelete
	}{
		{
			name:       "steady-state inserts write no deletes",
			upsertMode: true,
			ops:        []change{{"c", "1"}, {"c", "2"}},
		},
		{
			name:              "repeated insert of same id in same sync deletes the earlier insert by position",
			upsertMode:        true,
			ops:               []change{{"c", "1"}, {"c", "1"}},
			positionalDeletes: []PositionalDelete{{FilePath: dataFile, Position: 0}},
		},
		{
			name:              "insert then update in same sync deletes the insert by position",
			upsertMode:        true,
			ops:               []change{{"c", "1"}, {"u", "1"}},
			positionalDeletes: []PositionalDelete{{FilePath: dataFile, Position: 0}},
		},
		{
			name:              "insert then delete in same sync deletes the insert by position",
			upsertMode:        true,
			ops:               []change{{"c", "1"}, {"d", "1"}},
			positionalDeletes: []PositionalDelete{{FilePath: dataFile, Position: 0}},
		},
		{
			name:            "overlap-window insert writes equality delete",
			upsertMode:      true,
			ops:             []change{{"i", "1"}},
			equalityDeletes: []string{"1"},
		},
		{
			name:              "overlap-window insert then update keeps equality delete and deletes the insert by position",
			upsertMode:        true,
			ops:               []change{{"i", "1"}, {"u", "1"}},
			equalityDeletes:   []string{"1"},
			positionalDeletes: []PositionalDelete{{FilePath: dataFile, Position: 0}},
		},
		{
			name:            "update and delete write equality deletes",
			upsertMode:      true,
			ops:             []change{{"u", "1"}, {"d", "2"}},
			equalityDeletes: []string{"1", "2"},
		},
		{
			name:              "delete then re-insert in same sync keeps only the re-insert",
			upsertMode:        true,
			ops:               []change{{"d", "1"}, {"c", "1"}},
			equalityDeletes:   []string{"1"},
			positionalDeletes: []PositionalDelete{{FilePath: dataFile, Position: 0}},
		},
		{
			name:            "repeated updates keep only the latest by position",
			upsertMode:      true,
			ops:             []change{{"u", "1"}, {"u", "1"}, {"u", "1"}},
			equalityDeletes: []string{"1"},
			positionalDeletes: []PositionalDelete{
				{FilePath: dataFile, Position: 0},
				{FilePath: dataFile, Position: 1},
			},
		},
		{
			name:       "interleaved changes to inserted ids delete each previous occurrence",
			upsertMode: true,
			ops:        []change{{"c", "1"}, {"c", "2"}, {"u", "1"}, {"d", "2"}, {"u", "1"}},
			positionalDeletes: []PositionalDelete{
				{FilePath: dataFile, Position: 0},
				{FilePath: dataFile, Position: 1},
				{FilePath: dataFile, Position: 2},
			},
		},
		{
			name:       "snapshot rows are not tracked",
			upsertMode: true,
			ops:        []change{{"r", "1"}, {"r", "1"}},
		},
		{
			name: "append mode writes no deletes",
			ops:  []change{{"u", "1"}, {"u", "1"}, {"d", "2"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w, _, _ := newTestWriter(t, types.UpdateTypeEquality, tc.upsertMode, nil)
			records := make([]types.RawRecord, 0, len(tc.ops))
			for _, c := range tc.ops {
				records = append(records, testRecord(c.id, c.op, ""))
			}

			require.NoError(t, w.extract(context.Background(), records))

			writer := w.writers[""] // unpartitioned, so extract uses the writer under key ""
			assert.ElementsMatch(t, tc.equalityDeletes, writer.equalityDeletes)
			assert.ElementsMatch(t, tc.positionalDeletes, writer.positionalDeletes)
			for _, rec := range records {
				assert.NotEqual(t, "i", rec.OlakeColumns[constants.OpType], "op type i must be normalised to c")
			}
		})
	}
}

// A duplicate arriving after the data file rolled points its positional delete at the earlier file and row.
func TestExtractDuplicateAcrossDataFiles(t *testing.T) {
	ctx := context.Background()
	w, _, _ := newTestWriter(t, types.UpdateTypeEquality, true, nil)
	writer, err := w.getOrCreateWriter(ctx, "", nil)
	require.NoError(t, err)
	writer.dataWriter.currentRowCount = 5 // rows earlier batches wrote to the file
	firstFile := writer.dataWriter.filePath

	require.NoError(t, w.extract(ctx, []types.RawRecord{testRecord("1", "c", "a")}))

	// clear the batch as Write does, and drop the data file so the next extract opens a new one
	writer.data = writer.data[:0]
	writer.dataWriter = nil

	require.NoError(t, w.extract(ctx, []types.RawRecord{testRecord("1", "u", "b")}))
	assert.NotEqual(t, firstFile, writer.dataWriter.filePath)
	assert.Empty(t, writer.equalityDeletes)
	assert.Equal(t, []PositionalDelete{{FilePath: firstFile, Position: 5}}, writer.positionalDeletes)
	assert.Equal(t, PositionalDelete{FilePath: writer.dataWriter.filePath, Position: 0}, writer.olakeIDPosition["1"])
}
