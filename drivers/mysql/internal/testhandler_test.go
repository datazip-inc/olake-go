package driver

import (
	"github.com/datazip-inc/olake/drivers/mysql/internal/mysqltest"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// testConfig reaches this test's own database, which cleanup drops.
func testConfig(h *testhandler.Handler) *Config {
	return &Config{
		Host:     testhandler.Host,
		Port:     mysqltest.Source.Service().Port,
		Database: h.Database(mysqltest.Source),
		Username: "root",
		Password: mysqltest.RootPassword,
	}
}
