package driver

import (
	"github.com/datazip-inc/olake/drivers/mssql/internal/mssqltest"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// testConfig reaches this test's own database, which cleanup drops.
func testConfig(h *testhandler.Handler) *Config {
	return &Config{
		Host:     testhandler.Host,
		Port:     mssqltest.Source.Service().Port,
		Database: h.Database(mssqltest.Source),
		Username: "sa",
		Password: mssqltest.Password,
	}
}
