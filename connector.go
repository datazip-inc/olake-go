package olake

import (
	"os"

	"github.com/datazip-inc/olake/constants"
	_ "github.com/datazip-inc/olake/destination/iceberg" // registering iceberg destination
	_ "github.com/datazip-inc/olake/destination/parquet" // registering parquet destination
	"github.com/datazip-inc/olake/drivers/abstract"
	protocol "github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/safego"
)

func RegisterDriver(driver abstract.DriverInterface) {
	defer safego.Recovery(true)

	// Execute the root command
	err := protocol.CreateRootCommand(true, driver).Execute()
	if err != nil {
		protocol.ReportFailure(err)
		code := utils.ExitCode(err)
		hint := "please check the error and fix it before running the job again"
		if code == constants.ExitCodeRestartRequired {
			hint = "running the job again should fix this"
		}
		logger.Errorf("failed with exit code %d: %s. Error: %s", code, hint, err)
		os.Exit(code)
	}

	os.Exit(0)
}
