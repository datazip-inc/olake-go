package drivertest

import (
	"context"
	"errors"
	"strings"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// Kafka gives each test its own topic on the shared broker, named by the test id. Cleanup also deletes the
// consumer group of that name, so a test reads its topic with that group.
var Kafka testhandler.Source = kafkaSource{}

var kafkaService = &testhandler.Service{
	Container: "kafkaJson",
	Port:      29092,
	Target:    "olake.kafka.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		_, err := kafkaTool(ctx, s, "kafka-topics", "--list")
		return err
	},
}

type kafkaSource struct{}

func (kafkaSource) Service() *testhandler.Service {
	return kafkaService
}

// Create makes the test's topic with one partition, which keeps its messages in the order they were sent.
func (kafkaSource) Create(ctx context.Context, database string) error {
	_, err := kafkaTool(ctx, kafkaService, "kafka-topics", "--create", "--topic", database, "--partitions", "1", "--replication-factor", "1")
	return err
}

func (kafkaSource) Drop(ctx context.Context, database string) error {
	if _, err := kafkaTool(ctx, kafkaService, "kafka-topics", "--delete", "--if-exists", "--topic", database); err != nil {
		return err
	}
	out, err := kafkaTool(ctx, kafkaService, "kafka-consumer-groups", "--delete", "--group", database)
	// The tool exits 0 when the deletion fails too, so its output decides; a group nobody joined is no error.
	if err == nil && strings.Contains(out, "Error") && !strings.Contains(out, "GroupIdNotFoundException") {
		err = errors.New(out)
	}
	return err
}

// kafkaTool runs one of Kafka's command line tools in the broker's container against the broker itself.
func kafkaTool(ctx context.Context, s *testhandler.Service, tool string, args ...string) (string, error) {
	return s.Exec(ctx, nil, append([]string{tool, "--bootstrap-server", "localhost:9092"}, args...)...)
}
