package waljs

import (
	"context"
	"errors"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNumericScale(t *testing.T) {
	testCases := []struct {
		name   string
		packed int64
		want   int64
	}{
		{
			name:   "a positive scale",
			packed: 4,
			want:   4,
		},
		{
			name:   "a zero scale",
			packed: 0,
			want:   0,
		},
		{
			name:   "a negative scale postgres 15 packs as two's complement",
			packed: 2046,
			want:   -2,
		},
		{
			name:   "the most negative scale postgres allows",
			packed: 1048,
			want:   -1000,
		},
		{
			name:   "the largest scale postgres allows",
			packed: 1000,
			want:   1000,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, NumericScale(tc.packed))
		})
	}
}

func TestPgoutputReplicator_PrimaryKeyChanged(t *testing.T) {
	p := newTestPgoutputReplicator()
	rel := p.relationIDToMsgMap[1]
	null := &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
	toast := &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
	old := testTuple(textColumn("1"), textColumn("east"))
	newRow := testTuple(textColumn("2"), textColumn("east"))
	tests := []struct {
		name     string
		keys     []string
		old, new *pglogrepl.TupleData
		changed  bool
	}{
		{"single key changes", []string{"id"}, old, newRow, true},
		{"one composite key component changes", []string{"region", "id"}, old, newRow, true},
		{"ordinary update", []string{"region"}, old, newRow, false},
		{"no old tuple", []string{"id"}, nil, newRow, false},
		{"no primary key", nil, old, newRow, false},
		{"key absent from relation", []string{"absent"}, old, newRow, false},
		{"INDEX omits second key", []string{"id", "region"}, testTuple(textColumn("1"), null), newRow, false},
		{"INDEX omits first key", []string{"region", "id"}, testTuple(textColumn("1"), null), newRow, false},
		{"empty text key", []string{"region"}, testTuple(textColumn("1"), textColumn("")), newRow, true},
		{"unchanged TOAST key", []string{"region"}, old, testTuple(textColumn("2"), toast), false},
		{"same numeric key with different scale", []string{"id"}, old, testTuple(textColumn("1.00"), null), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &pglogrepl.UpdateMessage{OldTuple: tt.old, NewTuple: tt.new}
			values, _, _, err := p.tupleValuesToMap(rel, m.NewTuple, m.OldTuple)
			require.NoError(t, err)
			changed, err := p.primaryKeyChanged(rel, m, tt.keys, values)
			require.NoError(t, err)
			assert.Equal(t, tt.changed, changed)
		})
	}
}

func TestPgoutputReplicator_EmitUpdate(t *testing.T) {
	p := newTestPgoutputReplicator("id")
	p.preserveToastValues = true
	toast := &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
	null := &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
	next := testTuple(textColumn("2"), toast)
	oldRow := testTuple(textColumn("1"), textColumn("east"))
	oldKey := testTuple(textColumn("1"), null)

	type change struct {
		kind        string
		data        map[string]any
		unavailable []string
	}
	fullChanges := []change{
		{"delete", map[string]any{"id": 1.0, "region": "east"}, nil},
		{"update", map[string]any{"id": 2.0, "region": "east"}, nil},
	}
	keyChanges := []change{
		{"delete", map[string]any{"id": 1.0, "region": nil}, nil},
		{"update", map[string]any{"id": 2.0, "region": constants.UnavailableValue}, []string{"region"}},
	}
	writeErr := errors.New("destination write failed")
	testCases := []struct {
		name      string
		oldType   byte
		old       *pglogrepl.TupleData
		failOn    string
		want      []change
		wantError string
	}{
		{name: "FULL supplies old region", oldType: 'O', old: oldRow, want: fullChanges},
		{name: "DEFAULT supplies only old key", oldType: 'K', old: oldKey, want: keyChanges},
		{name: "ordinary update emits one event", want: keyChanges[1:]},
		{name: "failed delete stops the update", oldType: 'K', old: oldKey, failOn: "delete", want: keyChanges[:1]},
		{name: "failed update propagates the error", oldType: 'K', old: oldKey, failOn: "update", want: keyChanges},
		{name: "old key conversion fails before delivery", oldType: 'K', old: testTuple(textColumn("invalid"), null), wantError: "failed to compare primary key in stream public.orders"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: tc.oldType, OldTuple: tc.old, NewTuple: next}
			var got []change
			err := p.emitUpdate(t.Context(), msg, func(_ context.Context, event abstract.CDCChange) error {
				got = append(got, change{event.Kind, event.Data, event.UnavailableColumns})
				if event.Kind == tc.failOn {
					return writeErr
				}
				return nil
			})
			switch {
			case tc.failOn != "":
				require.ErrorIs(t, err, writeErr)
			case tc.wantError != "":
				require.ErrorContains(t, err, tc.wantError)
			default:
				require.NoError(t, err)
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestPgoutputReplicator_EmitDelete(t *testing.T) {
	p := newTestPgoutputReplicator("id")
	null := &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
	testCases := []struct {
		name string
		old  *pglogrepl.TupleData
		want map[string]any
	}{
		{"DEFAULT supplies the old key", testTuple(textColumn("1"), null), map[string]any{"id": 1.0, "region": nil}},
		{"INDEX supplies region without the primary key", testTuple(null, textColumn("east")), map[string]any{"id": nil, "region": "east"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := &pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: 'K', OldTuple: tc.old}
			var changes []abstract.CDCChange
			err := p.emitDelete(t.Context(), msg, func(_ context.Context, change abstract.CDCChange) error {
				changes = append(changes, change)
				return nil
			})
			require.NoError(t, err)
			require.Len(t, changes, 1)
			assert.Equal(t, "delete", changes[0].Kind)
			assert.Equal(t, tc.want, changes[0].Data)
		})
	}
}

func newTestPgoutputReplicator(keys ...string) *pgoutputReplicator {
	stream := types.NewStream("orders", "public", nil).WithPrimaryKey(keys...).Wrap(0)
	return &pgoutputReplicator{
		socket: &Socket{changeFilter: NewChangeFilter(func(value any, columnType string) (any, error) {
			if columnType == "numeric" {
				return typeutils.ReformatFloat64(value)
			}
			return value, nil
		}, stream)},
		relationIDToMsgMap: map[uint32]*pglogrepl.RelationMessage{
			1: {RelationID: 1, Namespace: "public", RelationName: "orders", Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: pgtype.NumericOID}, {Name: "region", DataType: pgtype.TextOID},
			}},
		},
	}
}

func testTuple(cols ...*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{Columns: cols}
}

func textColumn(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(s)}
}
