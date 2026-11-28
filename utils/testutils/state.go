package testutils

import (
	"path/filepath"
	"testing"

	"github.com/spf13/viper"

	"github.com/datazip-inc/olake/constants"
)

// UseStateFile points the state file at a temporary file until t ends. A driver writes it on every state
// change, and with no path set that write exits the test binary. Call it before t's parallel subtests start.
func UseStateFile(t testing.TB) {
	previous := viper.Get(constants.StatePath)
	viper.Set(constants.StatePath, filepath.Join(t.TempDir(), "state.json"))
	t.Cleanup(func() { viper.Set(constants.StatePath, previous) })
}
