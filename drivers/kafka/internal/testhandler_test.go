package driver

import (
	"github.com/datazip-inc/olake/drivers/kafka/internal/kafkatest"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// testConfig reads this test's own topic with a consumer group of the same name; cleanup deletes both.
func testConfig(h *testhandler.Handler) *Config {
	return &Config{
		BootstrapServers: kafkatest.Source.Service().Addr(),
		ConsumerGroupID:  h.Database(kafkatest.Source),
		Protocol:         ProtocolConfig{SecurityProtocol: "PLAINTEXT"},
		MaxThreads:       1,
	}
}
