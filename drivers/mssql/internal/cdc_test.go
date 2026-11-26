package driver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/mssql/internal/mssqltest"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/testutils"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

func TestMSSQLStreamChanges(t *testing.T) {
	testutils.UseStateFile(t)
	testCases := []struct {
		name       string
		statements []string
		want       []abstract.CDCChange
	}{
		{
			name:       "streams an insert",
			statements: []string{"INSERT INTO dbo.users VALUES (1, 'alice')"},
			want: []abstract.CDCChange{
				{Kind: "insert", Data: map[string]any{"id": int32(1), "name": "alice"}},
			},
		},
		{
			name: "streams an update with the new row",
			statements: []string{
				"INSERT INTO dbo.users VALUES (1, 'alice')",
				"UPDATE dbo.users SET name = 'bob' WHERE id = 1",
			},
			want: []abstract.CDCChange{
				{Kind: "insert", Data: map[string]any{"id": int32(1), "name": "alice"}},
				{Kind: "update", Data: map[string]any{"id": int32(1), "name": "bob"}},
			},
		},
		{
			name: "streams a delete with the old row",
			statements: []string{
				"INSERT INTO dbo.users VALUES (1, 'alice')",
				"DELETE FROM dbo.users WHERE id = 1",
			},
			want: []abstract.CDCChange{
				{Kind: "insert", Data: map[string]any{"id": int32(1), "name": "alice"}},
				{Kind: "delete", Data: map[string]any{"id": int32(1), "name": "alice"}},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Every case writes the same row to the same table, so a change leaking between databases shows in got.
			t.Parallel()
			config := testConfig(testhandler.New(t))
			require.NoError(t, mssqltest.Exec(t.Context(), config.Database, "CREATE TABLE dbo.users (id int PRIMARY KEY, name nvarchar(50))"))
			// Setup requires CDC on the database and a capture job, which the first captured table brings.
			require.NoError(t, mssqltest.EnableCDC(t.Context(), config.Database, "users"))
			config.UpdateMethod = map[string]any{"type": "CDC"}
			m := &MSSQL{config: config}
			require.NoError(t, m.Setup(t.Context()))
			t.Cleanup(func() { _ = m.Close() })
			m.SetupState(&types.State{RWMutex: &sync.RWMutex{}, Type: types.StreamType})
			require.NoError(t, m.PreCDC(t.Context(), []types.StreamInterface{types.NewStream("users", "dbo", nil).Wrap(0)}))
			for _, statement := range tc.statements {
				_, err := m.client.ExecContext(t.Context(), statement)
				require.NoError(t, err)
			}
			// SQL Server Agent copies the changes into the change table on its own schedule; operation 3 is an update's old row.
			require.Eventually(t, func() bool {
				var captured int
				err := m.client.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM cdc.dbo_users_CT WHERE __$operation <> 3").Scan(&captured)
				return err == nil && captured == len(tc.want)
			}, time.Minute, 500*time.Millisecond)

			var got []abstract.CDCChange
			_, err := m.StreamChanges(t.Context(), 0, nil, func(_ context.Context, change abstract.CDCChange) error {
				got = append(got, abstract.CDCChange{Kind: change.Kind, Data: change.Data})
				return nil
			})
			require.NoError(t, err)
			require.NoError(t, m.PostCDC(t.Context(), 0))
			require.Equal(t, tc.want, got)
		})
	}
}
