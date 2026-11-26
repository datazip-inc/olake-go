// Package postgrestest gives each unit test its own database on the Postgres server the integration suites share.
package postgrestest

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// Password is the password of drivers/postgres/docker-compose.yml, whose server the integration suites share.
const Password = "secret1234"

// Source gives each test its own database on the shared server, which is set up for logical replication.
var Source testhandler.Source = postgresSource{}

var postgresService = &testhandler.Service{
	Container: "olake_postgres-test",
	Port:      5433,
	Target:    "olake.postgres.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		return exec(ctx, s, "postgres", "SELECT 1")
	},
}

type postgresSource struct{}

func (postgresSource) Service() *testhandler.Service {
	return postgresService
}

func (postgresSource) Create(ctx context.Context, database string) error {
	return Exec(ctx, "postgres", "CREATE DATABASE "+pgx.Identifier{database}.Sanitize())
}

// Drop drops the database together with its replication slots and publications.
// A slot that is still streaming blocks the drop until its walsender ends, hence the retries.
func (postgresSource) Drop(ctx context.Context, database string) error {
	var err error
	for range 10 {
		err = Exec(ctx, "postgres",
			`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE database = '`+database+`' AND active`,
			"DROP DATABASE IF EXISTS "+pgx.Identifier{database}.Sanitize()+" WITH (FORCE)",
		)
		if err == nil {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return err
}

// Exec runs statements against one database of the shared server, each in its own transaction.
func Exec(ctx context.Context, database string, statements ...string) error {
	return exec(ctx, postgresService, database, statements...)
}

// exec takes the service as an argument: Ready sits in postgresService's initializer, which cannot refer to itself.
func exec(ctx context.Context, s *testhandler.Service, database string, statements ...string) error {
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://postgres:%s@%s/%s?sslmode=disable", Password, s.Addr(), database))
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
