// Package mysqltest gives each unit test its own database on the MySQL server the integration suites share.
package mysqltest

import (
	"context"
	"database/sql"

	"github.com/go-sql-driver/mysql"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// RootPassword is the root password of drivers/mysql/docker-compose.yml, whose server the integration suites share.
const RootPassword = "root1234"

// Source gives each test its own database on the shared server, which writes a row-format binlog.
var Source testhandler.Source = mysqlSource{}

var mysqlService = &testhandler.Service{
	Container: "olake_mysql-test",
	Port:      3306,
	Target:    "olake.mysql.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		return exec(ctx, s, "SELECT 1")
	},
}

type mysqlSource struct{}

func (mysqlSource) Service() *testhandler.Service {
	return mysqlService
}

func (mysqlSource) Create(ctx context.Context, database string) error {
	return exec(ctx, mysqlService, "CREATE DATABASE `"+database+"`")
}

// Drop gives up on a lock that a leaked transaction holds instead of hanging.
func (mysqlSource) Drop(ctx context.Context, database string) error {
	return exec(ctx, mysqlService, "SET SESSION lock_wait_timeout = 30", "DROP DATABASE IF EXISTS `"+database+"`")
}

// exec runs statements on the server as root, one after another in one session.
func exec(ctx context.Context, s *testhandler.Service, statements ...string) error {
	config := mysql.NewConfig()
	config.User, config.Passwd, config.Net, config.Addr = "root", RootPassword, "tcp", s.Addr()
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	for _, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
