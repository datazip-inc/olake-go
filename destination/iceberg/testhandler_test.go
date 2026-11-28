package iceberg

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/destination/destinationtest"
)

// sharedJVM connects a test to the writer JVM every unit test shares. Get the JVM ready first with
// testhandler's Destination(destinationtest.IcebergJVM), which also gives the test a namespace of its own.
func sharedJVM(t *testing.T) *serverInstance {
	server, err := connectServer(destinationtest.IcebergJVMAddr())
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.conn.Close() })
	return server
}
