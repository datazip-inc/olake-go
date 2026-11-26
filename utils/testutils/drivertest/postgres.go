// Package drivertest has the source databases unit tests read from, as testhandler.Source values.
// Their containers are the driver stacks the integration suites share, and cleanup drops one test's database from each.
package drivertest

import (
	"context"
	"time"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// PostgresPassword is the password of drivers/postgres/docker-compose.yml, whose server the integration suites share.
const PostgresPassword = "secret1234"

// Postgres gives each test its own database on the shared server, which is set up for logical replication.
var Postgres testhandler.Source = postgresSource{}

var postgresService = &testhandler.Service{
	Container: "olake_postgres-test",
	Port:      5433,
	Target:    "olake.postgres.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		_, err := psql(ctx, s, "postgres", "SELECT 1")
		return err
	},
}

type postgresSource struct{}

func (postgresSource) Service() *testhandler.Service {
	return postgresService
}

func (postgresSource) Create(ctx context.Context, database string) error {
	_, err := psql(ctx, postgresService, "postgres", `CREATE DATABASE "`+database+`"`)
	return err
}

// Drop drops the database together with its replication slots and publications.
// A slot that is still streaming blocks the drop until its walsender ends, hence the retries.
func (postgresSource) Drop(ctx context.Context, database string) error {
	var err error
	for range 10 {
		_, err = psql(ctx, postgresService, "postgres",
			`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE database = '`+database+`' AND active`,
			`DROP DATABASE IF EXISTS "`+database+`" WITH (FORCE)`,
		)
		if err == nil {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return err
}

// Psql runs statements against one database of the shared server, each in its own transaction.
func Psql(ctx context.Context, database string, statements ...string) (string, error) {
	return psql(ctx, postgresService, database, statements...)
}

// psql takes the service as an argument: Ready sits in postgresService's initializer, which cannot refer to itself.
func psql(ctx context.Context, s *testhandler.Service, database string, statements ...string) (string, error) {
	command := []string{
		"psql", "--host", "127.0.0.1", "--username", "postgres", "--dbname", database,
		"--no-psqlrc", "--quiet", "--tuples-only", "--no-align", "--set", "ON_ERROR_STOP=1",
	}
	for _, statement := range statements {
		command = append(command, "--command", statement)
	}
	return s.Exec(ctx, nil, command...)
}
