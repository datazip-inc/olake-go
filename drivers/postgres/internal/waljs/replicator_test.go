package waljs

import (
	"context"
	"net/url"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/postgres/internal/postgrestest"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

func TestReplicatorStreamChanges(t *testing.T) {
	testCases := []struct {
		name       string
		statements []string
		want       []abstract.CDCChange
	}{
		{
			name:       "streams an insert",
			statements: []string{`INSERT INTO users VALUES (1, 'alice')`},
			want: []abstract.CDCChange{
				{Kind: "insert", Data: map[string]any{"id": "1", "name": "alice"}},
			},
		},
		{
			name: "streams an update with the new row",
			statements: []string{
				`INSERT INTO users VALUES (1, 'alice')`,
				`UPDATE users SET name = 'bob' WHERE id = 1`,
			},
			want: []abstract.CDCChange{
				{Kind: "insert", Data: map[string]any{"id": "1", "name": "alice"}},
				{Kind: "update", Data: map[string]any{"id": "1", "name": "bob"}},
			},
		},
		{
			name: "streams a delete with only the key of the old row",
			statements: []string{
				`INSERT INTO users VALUES (1, 'alice')`,
				`DELETE FROM users WHERE id = 1`,
			},
			want: []abstract.CDCChange{
				{Kind: "insert", Data: map[string]any{"id": "1", "name": "alice"}},
				{Kind: "delete", Data: map[string]any{"id": "1", "name": nil}},
			},
		},
		{
			name:       "skips a published table the replicator was not given",
			statements: []string{`INSERT INTO orders VALUES (1)`},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			database := testhandler.New(t).Database(postgrestest.Source)
			connection := url.URL{
				Scheme:   "postgres",
				User:     url.UserPassword("postgres", postgrestest.Password),
				Host:     postgrestest.Source.Service().Addr(),
				Path:     "/" + database,
				RawQuery: "sslmode=disable",
			}
			db, err := sqlx.Open("pgx", connection.String())
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			exec := func(statement string, args ...any) {
				_, err := db.ExecContext(t.Context(), statement, args...)
				require.NoError(t, err)
			}
			exec(`CREATE TABLE users (id int PRIMARY KEY, name text)`)
			exec(`CREATE TABLE orders (id int PRIMARY KEY)`)
			exec(`CREATE PUBLICATION olake_publication FOR ALL TABLES`)
			// Named after the test's database, so cleanup drops the slot with it.
			exec(`SELECT pg_create_logical_replication_slot($1, 'pgoutput')`, database)
			for _, statement := range tc.statements {
				exec(statement)
			}
			slot, err := GetSlotPosition(t.Context(), db, database)
			require.NoError(t, err)

			replicator, err := NewReplicator(t.Context(), &Config{
				Tables:              types.NewSet[types.StreamInterface](types.NewStream("users", "public", nil).Wrap(0)),
				Connection:          connection,
				ReplicationSlotName: database,
				InitialWaitTime:     30 * time.Second,
				Publication:         "olake_publication",
			}, slot, nil, func(value any, _ string) (any, error) { return value, nil })
			require.NoError(t, err)
			t.Cleanup(func() { Cleanup(context.Background(), replicator.Socket()) })

			var got []abstract.CDCChange
			require.NoError(t, replicator.StreamChanges(t.Context(), db, func(_ context.Context, change abstract.CDCChange) error {
				got = append(got, abstract.CDCChange{Kind: change.Kind, Data: change.Data})
				return nil
			}))
			require.Equal(t, tc.want, got)
		})
	}
}
