package protocol

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/spf13/cobra"
)

// validateCheckFlags rejects a check with neither --config nor --destination,
// and a check with both: with both, the destination branch used to win silently
// and the source was never checked, so a broken source reported SUCCEEDED.
func validateCheckFlags() error {
	// If connector is not set, we are checking the destination
	if !configPassed() && !destinationPassed() {
		return errs.Precondition(errs.ConfigInvalid, codeFlagMissing,
			fmt.Errorf("no connector config or destination config provided"))
	}

	if configPassed() && destinationPassed() {
		return errs.Precondition(errs.ConfigInvalid, codeConflictingConfigFlags,
			fmt.Errorf("check accepts either --config or --destination, got both"))
	}

	return nil
}

// checkCmd represents the check command
var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "check command",
	PreRunE: func(_ *cobra.Command, _ []string) error {
		if err := validateCheckFlags(); err != nil {
			return err
		}

		// check for destination config
		if destinationConfigPath != "not-set" {
			destinationConfig = &types.WriterConfig{}
			return utils.UnmarshalFile(destinationConfigPath, destinationConfig, true)
		}

		// check for source config
		if configPath != "not-set" {
			return utils.UnmarshalFile(configPath, connector.GetConfigRef(), true)
		}

		return nil
	},
	// TODO: switch back to returning err once the worker handling is added for non-zero exit.
	Run: func(cmd *cobra.Command, _ []string) {
		var prerequisites types.PrerequisiteResults
		err := func() error {
			// If connector is not set, we are checking the destination
			if destinationConfigPath != "not-set" {
				// NewWriterPool initializes destination resources and runs Check;
				// close immediately since a check has no further work.
				pool, err := destination.NewWriterPool(cmd.Context(), destinationConfig, nil, batchSize)
				if err != nil {
					return err
				}
				pool.Shutdown(context.Background())
				return nil
			}

			if configPath != "not-set" {
				// Prerequisite results are reported alongside the status. Setup itself fails on unmet
				// required checks when the config selects CDC (abstract.RequireCDCPrerequisites).
				err := connector.Setup(cmd.Context())
				prerequisites = connector.Prerequisites()
				return err
			}

			return nil
		}()

		// A failed connection is a successful check: the verdict is this message, not the exit
		// code. Exiting non-zero fails the caller's activity before it ever parses the message.
		message := types.Message{
			Type: types.ConnectionStatusMessage,
			ConnectionStatus: &types.StatusRow{
				Status:        types.ConnectionSucceed,
				Prerequisites: prerequisites,
			},
		}
		if err != nil {
			message.ConnectionStatus.Message = err.Error()
			message.ConnectionStatus.Status = types.ConnectionFailed
		}
		logger.Info(message)

		// Reported here because check exits zero: RegisterDriver's hook only fires on a returned
		// error. PreRunE failures still reach it, so this cannot double-report.
		ReportFailure(err)
	},
}
