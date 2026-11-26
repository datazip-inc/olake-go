package drivertest

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// The root user of drivers/mongodb/docker-compose.yml, whose replica set the integration suites share.
const (
	MongoDBUser     = "admin"
	MongoDBPassword = "password"
)

// MongoDB gives each test its own database on the shared single-node replica set, so change streams work.
var MongoDB testhandler.Source = mongoSource{}

var mongoService = &testhandler.Service{
	Container: "primary_mongo",
	Port:      27017,
	Target:    "olake.mongodb.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		_, err := mongosh(ctx, s, `if (!db.hello().isWritablePrimary) { throw new Error("no primary yet") }`)
		return err
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
	_, err := mongosh(ctx, mongoService, fmt.Sprintf("db.getSiblingDB(%q).dropDatabase()", database))
	return err
}

// mongosh runs a script on the server as root; a thrown error fails the command.
func mongosh(ctx context.Context, s *testhandler.Service, script string) (string, error) {
	return s.Exec(ctx, nil, "mongosh", "--port", "27017", "--quiet",
		"--username", MongoDBUser, "--password", MongoDBPassword, "--authenticationDatabase", "admin", "--eval", script)
}
