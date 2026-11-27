package destinationtest

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

const jdbcCatalogDatabase = "olake_testhandler_catalog"

// jdbcCatalogService is the catalog's own Postgres, which creates jdbcCatalogDatabase when it first starts.
var jdbcCatalogService = &testhandler.Service{
	Container: "olake-catalog-postgres",
	Port:      15432,
	Target:    "testutils.postgres.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		_, err := psql(ctx, s, "SELECT 1")
		return err
	},
}

// IcebergJDBC writes Iceberg tables through a JDBC catalog kept in its own Postgres container.
var IcebergJDBC testhandler.DestinationKind = jdbcKind("iceberg-jdbc", nil)

// jdbcKind writes into the JDBC catalog and its warehouse; jvm is the writer its tests share, nil when each starts its own.
func jdbcKind(name string, jvm *testhandler.Service) *icebergKind {
	return &icebergKind{
		name:      name,
		catalog:   jdbcCatalogService,
		jvm:       jvm,
		warehouse: "s3a://" + bucket + "/iceberg_jdbc",
		config: map[string]any{
			"catalog_type":  "jdbc",
			"jdbc_url":      fmt.Sprintf("jdbc:postgresql://%s/%s", jdbcCatalogService.Addr(), jdbcCatalogDatabase),
			"jdbc_username": serverUser,
			"jdbc_password": serverKey,
		},
		dropNamespace: dropJDBCNamespace,
		files: func(namespace string) string {
			return "iceberg_jdbc/" + namespace
		},
	}
}

// dropJDBCNamespace removes a namespace and its tables from the catalog's two tables.
// The catalog creates those tables on first use, so they may not exist yet.
func dropJDBCNamespace(ctx context.Context, namespace string) error {
	_, err := psql(ctx, jdbcCatalogService, `DO $$ BEGIN
		IF to_regclass('iceberg_tables') IS NOT NULL THEN
			DELETE FROM iceberg_tables WHERE table_namespace = '`+namespace+`';
		END IF;
		IF to_regclass('iceberg_namespace_properties') IS NOT NULL THEN
			DELETE FROM iceberg_namespace_properties WHERE namespace = '`+namespace+`';
		END IF;
	END $$`)
	return err
}

// psql runs statements against the catalog's database over the container's socket, each in its own transaction.
func psql(ctx context.Context, s *testhandler.Service, statements ...string) (string, error) {
	command := []string{
		"psql", "--username", serverUser, "--dbname", jdbcCatalogDatabase,
		"--no-psqlrc", "--quiet", "--tuples-only", "--no-align", "--set", "ON_ERROR_STOP=1",
	}
	for _, statement := range statements {
		command = append(command, "--command", statement)
	}
	return s.Exec(ctx, nil, command...)
}
