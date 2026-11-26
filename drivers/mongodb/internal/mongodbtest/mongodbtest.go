// Package mongodbtest gives each unit test its own database on the MongoDB replica set the integration suites share.
package mongodbtest

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// The root user of drivers/mongodb/docker-compose.yml, whose replica set the integration suites share.
const (
	User     = "admin"
	Password = "password"
)

// Source gives each test its own database on the shared single-node replica set, so change streams work.
var Source testhandler.Source = mongoSource{}

var mongoService = &testhandler.Service{
	Container: "primary_mongo",
	Port:      27017,
	Target:    "olake.mongodb.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		return connect(ctx, s, func(client *mongo.Client) error {
			var hello struct {
				IsWritablePrimary bool `bson:"isWritablePrimary"`
			}
			if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
				return err
			}
			if !hello.IsWritablePrimary {
				return errors.New("no primary yet")
			}
			return nil
		})
	},
}

type mongoSource struct{}

func (mongoSource) Service() *testhandler.Service {
	return mongoService
}

// Create does nothing: MongoDB creates a database on its first write.
func (mongoSource) Create(context.Context, string) error {
	return nil
}

func (mongoSource) Drop(ctx context.Context, database string) error {
	return connect(ctx, mongoService, func(client *mongo.Client) error {
		return client.Database(database).Drop(ctx)
	})
}

// connect runs use with a root client of the member itself: the replica set advertises host.docker.internal,
// which a client on the host may not resolve.
func connect(ctx context.Context, s *testhandler.Service, use func(*mongo.Client) error) error {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+s.Addr()+"/?directConnection=true").
		SetAuth(options.Credential{Username: User, Password: Password, AuthSource: "admin"}))
	if err != nil {
		return err
	}
	defer func() { _ = client.Disconnect(context.Background()) }()
	return use(client)
}
