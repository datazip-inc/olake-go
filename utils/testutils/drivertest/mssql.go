package drivertest

import (
	"context"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// MSSQLPassword is the sa password of drivers/mssql/docker-compose.yml, whose server the integration suites share.
const MSSQLPassword = "Password!123"

// MSSQL gives each test its own database on the shared server, whose SQL Server Agent runs CDC capture jobs.
var MSSQL testhandler.Source = mssqlSource{}

var mssqlService = &testhandler.Service{
	Container: "olake-mssql",
	Port:      1433,
	Target:    "olake.mssql.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		_, err := sqlcmd(ctx, s, "master", "SELECT 1")
		return err
	},
}

type mssqlSource struct{}

func (mssqlSource) Service() *testhandler.Service {
	return mssqlService
}

func (mssqlSource) Create(ctx context.Context, database string) error {
	_, err := sqlcmd(ctx, mssqlService, "master", "CREATE DATABASE ["+database+"]")
	return err
}

// Drop first rolls back every other session, such as the one a CDC capture job holds on the database.
// Dropping the database also drops its CDC jobs, so it takes cdcJobLock.
func (mssqlSource) Drop(ctx context.Context, database string) error {
	_, err := sqlcmd(ctx, mssqlService, "master", cdcJobLock+"IF DB_ID(N'"+database+"') IS NOT NULL BEGIN "+
		"ALTER DATABASE ["+database+"] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ["+database+"] END")
	return err
}

// cdcJobLock makes the batches that add or drop a database's CDC jobs take turns across every test process:
// msdb deadlocks when two databases change their jobs at once. The batch's session holds it until sqlcmd exits.
const cdcJobLock = "DECLARE @lock int; EXEC @lock = sp_getapplock @Resource = N'olake_ut_cdc_jobs', @LockMode = N'Exclusive', " +
	"@LockOwner = N'Session', @LockTimeout = 300000; IF @lock < 0 THROW 50000, N'no turn on the CDC job lock', 1; "

// EnableMSSQLCDC captures the changes of a dbo table in a test's database. SQL Server adds the database's CDC jobs
// with its first captured table.
func EnableMSSQLCDC(ctx context.Context, database, table string) error {
	_, err := sqlcmd(ctx, mssqlService, "master", cdcJobLock+"USE ["+database+"]; EXEC sys.sp_cdc_enable_db; "+
		"EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'"+table+"', @role_name = NULL")
	return err
}

// Sqlcmd runs a batch of statements against one database of the shared server.
func Sqlcmd(ctx context.Context, database, statements string) (string, error) {
	return sqlcmd(ctx, mssqlService, database, statements)
}

// sqlcmd takes the service as an argument: Ready sits in mssqlService's initializer, which cannot refer to itself.
func sqlcmd(ctx context.Context, s *testhandler.Service, database, statements string) (string, error) {
	return s.Exec(ctx, []string{"--env", "SQLCMDPASSWORD=" + MSSQLPassword}, "/opt/mssql-tools18/bin/sqlcmd",
		"-S", "localhost", "-U", "sa", "-C", "-b", "-h", "-1", "-W", "-d", database, "-Q", "SET NOCOUNT ON; "+statements)
}
