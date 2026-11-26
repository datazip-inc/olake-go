package driver

import (
	"github.com/datazip-inc/olake/utils/testutils/drivertest"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// testConfig reaches this test's own database, which cleanup drops. It connects to the member directly:
// the replica set advertises host.docker.internal, which a client on the host may not resolve.
func testConfig(h *testhandler.Handler) *Config {
	return &Config{
		Hosts:            []string{drivertest.MongoDB.Service().Addr()},
		AuthDB:           "admin",
		Database:         h.Database(drivertest.MongoDB),
		Username:         drivertest.MongoDBUser,
		Password:         drivertest.MongoDBPassword,
		AdditionalParams: map[string]string{"directConnection": "true"},
	}
}
