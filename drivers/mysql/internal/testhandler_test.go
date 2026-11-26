package driver

import (
	"github.com/datazip-inc/olake/utils/testutils/drivertest"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// testConfig reaches this test's own database, which cleanup drops.
func testConfig(h *testhandler.Handler) *Config {
	return &Config{
		Host:     testhandler.Host,
		Port:     drivertest.MySQL.Service().Port,
		Database: h.Database(drivertest.MySQL),
		Username: "root",
		Password: drivertest.MySQLRootPassword,
	}
}
