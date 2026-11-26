package driver

import (
	"github.com/datazip-inc/olake/utils/testutils/drivertest"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// testConfig reads this test's own topic with a consumer group of the same name; cleanup deletes both.
func testConfig(h *testhandler.Handler) *Config {
	return &Config{
		BootstrapServers: drivertest.Kafka.Service().Addr(),
		ConsumerGroupID:  h.Database(drivertest.Kafka),
		Protocol:         ProtocolConfig{SecurityProtocol: "PLAINTEXT"},
		MaxThreads:       1,
	}
}
