package destinationtest

import (
	"context"
	"errors"
	"fmt"

	"github.com/datazip-inc/olake/utils/testutils/drivertest"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

const jdbcCatalogDatabase = "olake_testhandler_catalog"

// IcebergJDBC writes Iceberg tables through a JDBC catalog kept in the Postgres container.
var IcebergJDBC testhandler.DestinationKind = &icebergKind{
	name:      "iceberg-jdbc",
	catalog:   drivertest.Postgres.Service(),
	prepare:   createJDBCCatalog,
	warehouse: "s3a://" + bucket + "/iceberg_jdbc",
	config: map[string]any{
		"catalog_type":  "jdbc",
		"jdbc_url":      fmt.Sprintf("jdbc:postgresql://%s/%s", drivertest.Postgres.Service().Addr(), jdbcCatalogDatabase),
		"jdbc_username": "postgres",
		"jdbc_password": drivertest.PostgresPassword,
	},
	dropNamespace: dropJDBCNamespace,
	files: func(namespace string) string {
		return "iceberg_jdbc/" + namespace
	},
}

// createJDBCCatalog creates the catalog's database. Another process may create it at the same moment,
// so only its absence is an error.
func createJDBCCatalog(ctx context.Context) error {
	_, createErr := drivertest.Psql(ctx, "postgres", `CREATE DATABASE `+jdbcCatalogDatabase)
	found, err := drivertest.Psql(ctx, "postgres", `SELECT count(*) FROM pg_database WHERE datname = '`+jdbcCatalogDatabase+`'`)
	if err != nil || found != "1" {
		return fmt.Errorf("no JDBC catalog database: %v", errors.Join(createErr, err))
	}
	return nil
}

// dropJDBCNamespace removes a namespace and its tables from the catalog's two tables.
// The catalog creates those tables on first use, so they may not exist yet.
func dropJDBCNamespace(ctx context.Context, namespace string) error {
	_, err := drivertest.Psql(ctx, jdbcCatalogDatabase, `DO $$ BEGIN
		IF to_regclass('iceberg_tables') IS NOT NULL THEN
			DELETE FROM iceberg_tables WHERE table_namespace = '`+namespace+`';
		END IF;
		IF to_regclass('iceberg_namespace_properties') IS NOT NULL THEN
			DELETE FROM iceberg_namespace_properties WHERE namespace = '`+namespace+`';
		END IF;
	END $$`)
	return err
}
