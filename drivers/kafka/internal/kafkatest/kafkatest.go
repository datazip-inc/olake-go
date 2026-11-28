// Package kafkatest gives each unit test its own topic on the Kafka broker the integration suites share.
package kafkatest

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// Source gives each test its own topic on the shared broker, named by the test id. Cleanup also deletes the
// consumer group of that name, so a test reads its topic with that group.
var Source testhandler.Source = kafkaSource{}

var kafkaService = &testhandler.Service{
	Container: "kafkaJson",
	Port:      29092,
	Target:    "olake.kafka.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		return admin(s, func(adm *kadm.Client) error {
			_, err := adm.ListBrokers(ctx)
			return err
		})
	},
}

type kafkaSource struct{}

func (kafkaSource) Service() *testhandler.Service {
	return kafkaService
}

// Create makes the test's topic with one partition, which keeps its messages in the order they were sent.
func (kafkaSource) Create(ctx context.Context, database string) error {
	return admin(kafkaService, func(adm *kadm.Client) error {
		created, err := adm.CreateTopic(ctx, 1, 1, nil, database)
		if err != nil {
			return err
		}
		return created.Err
	})
}

// Drop deletes the test's topic and its consumer group; a test may have made neither.
func (kafkaSource) Drop(ctx context.Context, database string) error {
	return admin(kafkaService, func(adm *kadm.Client) error {
		topics, err := adm.DeleteTopics(ctx, database)
		if err != nil {
			return err
		}
		if err := topics[database].Err; err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
			return err
		}
		groups, err := adm.DeleteGroups(ctx, database)
		if err != nil {
			return err
		}
		if err := groups[database].Err; err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
			return err
		}
		return nil
	})
}

// admin runs use with an admin client of the broker, closed when use returns.
func admin(s *testhandler.Service, use func(*kadm.Client) error) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(s.Addr()))
	if err != nil {
		return err
	}
	defer client.Close()
	return use(kadm.NewClient(client))
}
