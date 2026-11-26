package driver

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/testutils/drivertest"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// SupportedUpdateTypes runs before Setup, so it reads the raw update_method, never cdcConfig.
func TestSupportedUpdateTypes(t *testing.T) {
	indexed := []types.UpdateType{types.UpdateTypePosition, types.UpdateTypeDeletionVector}

	tests := []struct {
		name         string
		updateMethod any
		cdcConfig    CDC
		want         []types.UpdateType
	}{
		{name: "no update method has no restriction"},
		{name: "standard replication has no restriction", updateMethod: map[string]any{"type": "Standard"}},
		{name: "cdc without preserve_toast_values has no restriction", updateMethod: map[string]any{"type": "CDC", "replication_slot": "olake_slot"}},
		{
			name:         "preserve_toast_values off has no restriction",
			updateMethod: map[string]any{"type": "CDC", "replication_slot": "olake_slot", "preserve_toast_values": false},
		},
		{
			name:         "preserve_toast_values offers only the indexed formats",
			updateMethod: map[string]any{"type": "CDC", "replication_slot": "olake_slot", "preserve_toast_values": true},
			want:         indexed,
		},
		{
			name:         "preserve_toast_values alone decides, without a replication slot",
			updateMethod: map[string]any{"preserve_toast_values": true},
			want:         indexed,
		},
		{
			name:         "preserve_toast_values of the wrong type is left to Setup",
			updateMethod: map[string]any{"type": "CDC", "replication_slot": "olake_slot", "preserve_toast_values": "true"},
		},
		{name: "update method that is not an object is left to Setup", updateMethod: "CDC"},
		{
			name:      "parsed cdc config is not read before Setup",
			cdcConfig: CDC{ReplicationSlot: "olake_slot", PreserveToastValues: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &Postgres{config: &Config{UpdateMethod: tc.updateMethod}, cdcConfig: tc.cdcConfig}
			assert.Equal(t, tc.want, p.SupportedUpdateTypes())
		})
	}
}

// SupportedWriteModes runs after Setup, so it reads the cdcConfig Setup parsed, never the raw update_method.
func TestSupportedWriteModes(t *testing.T) {
	tests := []struct {
		name         string
		updateMethod any
		cdcConfig    CDC
		want         []types.WriteMode
	}{
		{name: "standard replication has no restriction"},
		{name: "cdc without preserve_toast_values has no restriction", cdcConfig: CDC{ReplicationSlot: "olake_slot"}},
		{
			name:      "preserve_toast_values offers only upsert",
			cdcConfig: CDC{ReplicationSlot: "olake_slot", PreserveToastValues: true},
			want:      []types.WriteMode{types.WriteModeUpsert},
		},
		{
			name:         "raw update method is not read",
			updateMethod: map[string]any{"type": "CDC", "replication_slot": "olake_slot", "preserve_toast_values": true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &Postgres{config: &Config{UpdateMethod: tc.updateMethod}, cdcConfig: tc.cdcConfig}
			assert.Equal(t, tc.want, p.SupportedWriteModes())
		})
	}
}

func TestPostgresStreamChanges(t *testing.T) {
	drivertest.UseStateFile(t)
	testCases := []struct {
		name       string
		statements []string
		want       []abstract.CDCChange
	}{
		{
			name:       "streams an insert",
			statements: []string{`INSERT INTO users VALUES (1, 'alice')`},
			want: []abstract.CDCChange{
				{Kind: "insert", Data: map[string]any{"id": int32(1), "name": "alice"}},
			},
		},
		{
			name: "streams an update with the new row",
			statements: []string{
				`INSERT INTO users VALUES (1, 'alice')`,
				`UPDATE users SET name = 'bob' WHERE id = 1`,
			},
			want: []abstract.CDCChange{
				{Kind: "insert", Data: map[string]any{"id": int32(1), "name": "alice"}},
				{Kind: "update", Data: map[string]any{"id": int32(1), "name": "bob"}},
			},
		},
		{
			name: "streams a delete with only the key of the old row",
			statements: []string{
				`INSERT INTO users VALUES (1, 'alice')`,
				`DELETE FROM users WHERE id = 1`,
			},
			want: []abstract.CDCChange{
				{Kind: "insert", Data: map[string]any{"id": int32(1), "name": "alice"}},
				{Kind: "delete", Data: map[string]any{"id": int32(1), "name": nil}},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Every case writes the same row to the same table, so a change leaking between databases shows in got.
			t.Parallel()
			config := testConfig(testhandler.New(t))
			// The slot is named after the test's database, so cleanup drops it with the database.
			_, err := drivertest.Psql(t.Context(), config.Database,
				`CREATE TABLE users (id int PRIMARY KEY, name text)`,
				`CREATE PUBLICATION olake_publication FOR TABLE users`,
				`SELECT pg_create_logical_replication_slot('`+config.Database+`', 'pgoutput')`)
			require.NoError(t, err)
			config.UpdateMethod = map[string]any{"replication_slot": config.Database, "publication": "olake_publication", "initial_wait_time": 120}
			p := &Postgres{config: config}
			require.NoError(t, p.Setup(t.Context()))
			t.Cleanup(p.CloseConnection)
			p.SetupState(&types.State{RWMutex: &sync.RWMutex{}, Type: types.GlobalType})
			require.NoError(t, p.PreCDC(t.Context(), []types.StreamInterface{types.NewStream("users", "public", nil).Wrap(0)}))
			_, err = drivertest.Psql(t.Context(), config.Database, tc.statements...)
			require.NoError(t, err)

			var got []abstract.CDCChange
			_, err = p.StreamChanges(t.Context(), 0, nil, func(_ context.Context, change abstract.CDCChange) error {
				got = append(got, abstract.CDCChange{Kind: change.Kind, Data: change.Data})
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, p.PostCDC(t.Context(), 0))
			require.Equal(t, tc.want, got)
		})
	}
}
