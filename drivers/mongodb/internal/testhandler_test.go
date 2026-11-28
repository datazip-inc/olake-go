package driver

import (
	"github.com/datazip-inc/olake/drivers/mongodb/internal/mongodbtest"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// testConfig reaches this test's own database, which cleanup drops. It connects to the member directly:
// the replica set advertises host.docker.internal, which a client on the host may not resolve.
func testConfig(h *testhandler.Handler) *Config {
	return &Config{
		Hosts:            []string{mongodbtest.Source.Service().Addr()},
		AuthDB:           "admin",
		Database:         h.Database(mongodbtest.Source),
		Username:         mongodbtest.User,
		Password:         mongodbtest.Password,
		AdditionalParams: map[string]string{"directConnection": "true"},
	}
}
