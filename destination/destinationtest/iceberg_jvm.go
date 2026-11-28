package destinationtest

import (
	"context"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// icebergJVMService is the Iceberg writer JVM that destination/iceberg/local-test/catalog/docker-compose.yml runs
// on the JDBC catalog and its warehouse, the ones IcebergJDBC uses.
var icebergJVMService = &testhandler.Service{
	Container: "olake-catalog-iceberg-writer",
	Port:      15051,
	Target:    "testutils.iceberg-writer.start",
	// Probed inside the container: Docker's port proxy accepts host connections before the JVM listens.
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		_, err := s.Exec(ctx, nil, "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/50051")
		return err
	},
}

// IcebergJVM writes into IcebergJDBC's catalog and warehouse through the writer JVM every test shares,
// so a test calls the writer's gRPC services at IcebergJVMAddr instead of starting a JVM of its own.
var IcebergJVM testhandler.DestinationKind = jdbcKind("iceberg-jvm", icebergJVMService)

// IcebergJVMAddr is the host address of the shared writer's gRPC services.
func IcebergJVMAddr() string {
	return icebergJVMService.Addr()
}
