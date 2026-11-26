// Package mssqltest gives each unit test its own database on the SQL Server the integration suites share.
package mssqltest

import (
	"context"
	"database/sql"
	"net/url"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// Password is the sa password of drivers/mssql/docker-compose.yml, whose server the integration suites share.
const Password = "Password!123"

// Source gives each test its own database on the shared server, whose SQL Server Agent runs CDC capture jobs.
var Source testhandler.Source = mssqlSource{}

var mssqlService = &testhandler.Service{
	Container: "olake-mssql",
	Port:      1433,
	Target:    "olake.mssql.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		return exec(ctx, s, "master", "SELECT 1")
	},
}

type mssqlSource struct{}

func (mssqlSource) Service() *testhandler.Service {
	return mssqlService
}

func (mssqlSource) Create(ctx context.Context, database string) error {
	return Exec(ctx, "master", "CREATE DATABASE ["+database+"]")
}

// Drop first rolls back every other session, such as the one a CDC capture job holds on the database.
// Dropping the database also drops its CDC jobs, so it takes cdcJobLock.
func (mssqlSource) Drop(ctx context.Context, database string) error {
	return Exec(ctx, "master", cdcJobLock+"IF DB_ID(N'"+database+"') IS NOT NULL BEGIN "+
		"ALTER DATABASE ["+database+"] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ["+database+"] END")
}

// cdcJobLock makes the batches that add or drop a database's CDC jobs take turns across every test process:
// msdb deadlocks when two databases change their jobs at once. The batch's session holds it until exec returns.
const cdcJobLock = "DECLARE @lock int; EXEC @lock = sp_getapplock @Resource = N'olake_ut_cdc_jobs', @LockMode = N'Exclusive', " +
	"@LockOwner = N'Session', @LockTimeout = 300000; IF @lock < 0 THROW 50000, N'no turn on the CDC job lock', 1; "

// EnableCDC captures the changes of a dbo table in a test's database. SQL Server adds the database's CDC jobs
// with its first captured table.
func EnableCDC(ctx context.Context, database, table string) error {
	return Exec(ctx, "master", cdcJobLock+"USE ["+database+"]; EXEC sys.sp_cdc_enable_db; "+
		"EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'"+table+"', @role_name = NULL")
}

// Exec runs a batch of statements against one database of the shared server.
func Exec(ctx context.Context, database, batch string) error {
	return exec(ctx, mssqlService, database, batch)
}

// exec runs the batch in a session of its own, which ends when it returns and so drops any lock the batch took.
// It takes the service as an argument: Ready sits in mssqlService's initializer, which cannot refer to itself.
func exec(ctx context.Context, s *testhandler.Service, database, batch string) error {
	uri := url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword("sa", Password),
		Host:     s.Addr(),
		RawQuery: url.Values{"database": {database}, "encrypt": {"disable"}}.Encode(),
	}
	connector, err := mssql.NewConnector(uri.String())
	if err != nil {
		return err
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	_, err = db.ExecContext(ctx, batch)
	return err
}
