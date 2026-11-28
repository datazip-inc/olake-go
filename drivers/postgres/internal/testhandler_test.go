package driver

import (
	"github.com/datazip-inc/olake/drivers/postgres/internal/postgrestest"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// testConfig reaches this test's own database, which cleanup drops.
func testConfig(h *testhandler.Handler) *Config {
	return &Config{
		Host:     testhandler.Host,
		Port:     postgrestest.Source.Service().Port,
		Database: h.Database(postgrestest.Source),
		Username: "postgres",
		Password: postgrestest.Password,
	}
}
