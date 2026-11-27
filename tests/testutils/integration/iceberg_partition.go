package integration

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// The partition column of these suites: every seed row has col_char 'c', and the "update" query moves row 1 to 'd'.
const (
	movingPartitionRegex  = "/{col_char,identity}"
	evolvedPartitionRegex = "/{col_char,identity}/{col_bigserial,bucket[2]}"
	evolvedAppliedFields  = "col_char:identity;col_bigserial:bucket[2]"
	// appliedFieldsProperty is the table property the Java writer records the partition fields it applied in.
	appliedFieldsProperty = "olake.partition-fields"
)

// TestIcebergPartitionChanges covers rows whose partition changes: an update that moves a row to another partition, a
// partition_regex change that evolves the table's partition spec in place, and a spec evolved by another engine. Every
// delete has to carry the spec and partition of the data file holding the row it supersedes, or readers that filter on
// the partition column still see the old row and Iceberg 1.12+ refuses the table.
func (th *TestHandler) TestIcebergPartitionChanges(t *testing.T) {
	ctx := t.Context()
	testTable := th.GetTableName()

	writerTypes := []struct {
		name     string
		useArrow bool
	}{
		{"Legacy", false},
		{"Arrow", true},
	}
	for _, wt := range writerTypes {
		for _, updateType := range []string{"pos", "dv"} {
			t.Run(wt.name+"/"+updateType+"/move", func(t *testing.T) {
				require.NoError(t, th.IcebergWriter(ctx, t, testTable, wt.useArrow, func(ctx context.Context, t *testing.T, table string) error {
					return th.testIcebergPartitionMove(ctx, t, table, updateType)
				}))
			})
			t.Run(wt.name+"/"+updateType+"/evolve", func(t *testing.T) {
				require.NoError(t, th.IcebergWriter(ctx, t, testTable, wt.useArrow, func(ctx context.Context, t *testing.T, table string) error {
					return th.testIcebergPartitionEvolution(ctx, t, table, updateType)
				}))
			})
		}
	}
	// the arrow writer computes partitions from partition_regex, so only the legacy writer follows a spec another engine set
	for _, updateType := range []string{"pos", "dv"} {
		t.Run("Legacy/"+updateType+"/external", func(t *testing.T) {
			require.NoError(t, th.IcebergWriter(ctx, t, testTable, false, func(ctx context.Context, t *testing.T, table string) error {
				return th.testIcebergExternalPartitionEvolution(ctx, t, table, updateType)
			}))
		})
	}
	t.Run("Legacy/eq/keeps-spec", func(t *testing.T) {
		require.NoError(t, th.IcebergWriter(ctx, t, testTable, false, th.testIcebergEqualityKeepsPartitionSpec))
	})

	if testutils.KeepTestData() {
		t.Logf("keeping %s source data for PartitionChanges as (%s) is set", th.Driver, testutils.KeepTestDataEnvVar)
		return
	}
	th.ExecuteQuery(ctx, t, th.TestConfig, "drop")
}

// setPartitionRegex sets partition_regex in selected_streams for the stream identified by namespace+streamName.
func setPartitionRegex(config *testutils.TestConfig, namespace, streamName, regex string) error {
	return editSelectedStream(config, namespace, streamName, func(entry map[string]interface{}) {
		entry["partition_regex"] = regex
	})
}

// preparePartitionSync resets the source and catalog for a CDC stream with the given update type and partition_regex,
// and runs the full load.
func (th *TestHandler) preparePartitionSync(ctx context.Context, t *testing.T, testTable, updateType, regex string) error {
	t.Helper()
	if err := th.prepareTableIndexSync(ctx, t, testTable); err != nil {
		return err
	}
	if err := setUpdateType(th.TestConfig, th.Namespace, testTable, updateType); err != nil {
		return fmt.Errorf("failed setting update type: %w", err)
	}
	if err := setPartitionRegex(th.TestConfig, th.Namespace, testTable, regex); err != nil {
		return fmt.Errorf("failed setting partition_regex: %w", err)
	}
	return th.runIcebergSync(ctx, t, "initial full load", false)
}

// testIcebergPartitionMove updates row 1 from partition col_char=c to col_char=d. The old copy has to be deleted in its
// own partition: stamped with the new one, a delete file spanning several data files misses it on every read, and a
// file-scoped delete or deletion vector misses it under a filter on col_char.
func (th *TestHandler) testIcebergPartitionMove(ctx context.Context, t *testing.T, testTable, updateType string) error {
	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, th.TestConfig.DestinationDB, testTable)
	defer testutils.DropIcebergTable(t, testTable, th.TestConfig.DestinationDB)

	if err := th.preparePartitionSync(ctx, t, testTable, updateType, movingPartitionRegex); err != nil {
		return err
	}
	th.applyCDCUpdate(ctx, t)
	if err := th.runIcebergSync(ctx, t, "cdc sync moving row 1", true); err != nil {
		return err
	}

	spark := getSparkSession(ctx, t)
	th.assertMovedRow(ctx, t, spark, fullTableName, seedRowCount)
	require.Equal(t, []string{"c"}, sparkStrings(ctx, t, spark, fmt.Sprintf(
		"SELECT DISTINCT cast(partition.col_char AS string) AS v FROM %s.delete_files", fullTableName)),
		"the delete is stamped with the partition the row moved out of")
	return nil
}

// testIcebergPartitionEvolution changes partition_regex between two syncs. The table's spec is evolved in place, rows
// written after the change land under the new spec, and the update into a file of the old spec is deleted under that
// spec.
func (th *TestHandler) testIcebergPartitionEvolution(ctx context.Context, t *testing.T, testTable, updateType string) error {
	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, th.TestConfig.DestinationDB, testTable)
	defer testutils.DropIcebergTable(t, testTable, th.TestConfig.DestinationDB)

	if err := th.preparePartitionSync(ctx, t, testTable, updateType, movingPartitionRegex); err != nil {
		return err
	}
	if err := setPartitionRegex(th.TestConfig, th.Namespace, testTable, evolvedPartitionRegex); err != nil {
		return fmt.Errorf("failed changing partition_regex: %w", err)
	}
	th.applyCDCUpdate(ctx, t)
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "insert")
	if err := th.runIcebergSync(ctx, t, "cdc sync after the partition_regex change", true); err != nil {
		return err
	}

	spark := getSparkSession(ctx, t)
	require.Equal(t, evolvedAppliedFields, tableProperty(ctx, t, spark, fullTableName, appliedFieldsProperty))
	require.Equal(t, []string{"0", "1"}, sparkStrings(ctx, t, spark, fmt.Sprintf(
		"SELECT DISTINCT cast(spec_id AS string) AS v FROM %s.data_files", fullTableName)),
		"the full load stays under the old spec, the cdc rows land under the evolved one")
	require.Equal(t, []string{"0"}, sparkStrings(ctx, t, spark, fmt.Sprintf(
		"SELECT DISTINCT cast(spec_id AS string) AS v FROM %s.delete_files", fullTableName)),
		"the update superseded a row in a file of the old spec")
	th.assertMovedRow(ctx, t, spark, fullTableName, seedRowCount+1)
	return nil
}

// testIcebergExternalPartitionEvolution evolves the spec from Spark between two syncs. OLake keeps writing under that
// spec while its own partition_regex is unchanged, and deletes into files of the old spec carry the old spec.
func (th *TestHandler) testIcebergExternalPartitionEvolution(ctx context.Context, t *testing.T, testTable, updateType string) error {
	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, th.TestConfig.DestinationDB, testTable)
	defer testutils.DropIcebergTable(t, testTable, th.TestConfig.DestinationDB)

	if err := th.preparePartitionSync(ctx, t, testTable, updateType, movingPartitionRegex); err != nil {
		return err
	}
	spark := getSparkSession(ctx, t)
	runSpark(ctx, t, spark, fmt.Sprintf("ALTER TABLE %s ADD PARTITION FIELD bucket(2, col_bigserial)", fullTableName))

	th.applyCDCUpdate(ctx, t)
	if err := th.runIcebergSync(ctx, t, "cdc sync after the external spec change", true); err != nil {
		return err
	}

	require.Equal(t, []string{"0", "1"}, sparkStrings(ctx, t, spark, fmt.Sprintf(
		"SELECT DISTINCT cast(spec_id AS string) AS v FROM %s.data_files", fullTableName)),
		"the spec another engine evolved is kept")
	require.Equal(t, []string{"0"}, sparkStrings(ctx, t, spark, fmt.Sprintf(
		"SELECT DISTINCT cast(spec_id AS string) AS v FROM %s.delete_files", fullTableName)))
	th.assertMovedRow(ctx, t, spark, fullTableName, seedRowCount)
	return nil
}

// testIcebergEqualityKeepsPartitionSpec changes partition_regex of an eq stream: an equality delete written under a new
// spec never reaches a row in a file of the old one, so the table keeps its spec until it is rebuilt.
func (th *TestHandler) testIcebergEqualityKeepsPartitionSpec(ctx context.Context, t *testing.T, testTable string) error {
	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, th.TestConfig.DestinationDB, testTable)
	defer testutils.DropIcebergTable(t, testTable, th.TestConfig.DestinationDB)

	if err := th.preparePartitionSync(ctx, t, testTable, "eq", movingPartitionRegex); err != nil {
		return err
	}
	if err := setPartitionRegex(th.TestConfig, th.Namespace, testTable, evolvedPartitionRegex); err != nil {
		return fmt.Errorf("failed changing partition_regex: %w", err)
	}
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "insert")
	if err := th.runIcebergSync(ctx, t, "cdc sync after the partition_regex change", true); err != nil {
		return err
	}

	spark := getSparkSession(ctx, t)
	require.Equal(t, []string{"0"}, sparkStrings(ctx, t, spark, fmt.Sprintf(
		"SELECT DISTINCT cast(spec_id AS string) AS v FROM %s.data_files", fullTableName)), "the spec is not evolved")
	require.Equal(t, "col_char:identity", tableProperty(ctx, t, spark, fullTableName, appliedFieldsProperty))
	require.Equal(t, seedRowCount+1, countLiveRecords(ctx, t, spark, fullTableName))
	return nil
}

// assertMovedRow checks row 1 was moved to col_char=d without leaving a copy in col_char=c, on a plain read and on reads
// filtered by the partition column.
func (th *TestHandler) assertMovedRow(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string, liveRows int64) {
	t.Helper()
	require.Equal(t, liveRows, countLiveRecords(ctx, t, spark, fullTableName), "live rows")
	require.Equal(t, []string{"d"}, sparkStrings(ctx, t, spark, fmt.Sprintf(
		"SELECT cast(col_char AS string) AS v FROM %s WHERE col_bigserial = 1", fullTableName)), "row 1 has one live copy, moved")
	require.Equal(t, seedRowCount-1, countSpark(ctx, t, spark, fmt.Sprintf(
		"SELECT count(*) AS cnt FROM %s WHERE col_char = 'c'", fullTableName)), "no copy of row 1 is left in col_char=c")
	require.Equal(t, int64(0), countSpark(ctx, t, spark, fmt.Sprintf(
		"SELECT count(*) AS cnt FROM %s WHERE col_char = 'c' AND col_bigserial = 1", fullTableName)))
}

// sparkStrings runs a query selecting one string column v and returns its values sorted.
func sparkStrings(ctx context.Context, t *testing.T, spark sql.SparkSession, query string) []string {
	t.Helper()
	df, err := spark.Sql(ctx, query)
	require.NoError(t, err, "spark sql failed: %s", query)
	rows, err := df.Collect(ctx)
	require.NoError(t, err, "spark collect failed: %s", query)
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		values = append(values, fmt.Sprintf("%v", row.Value("v")))
	}
	slices.Sort(values)
	return values
}

// tableProperty returns one table property, or "" when the table does not have it.
func tableProperty(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName, key string) string {
	t.Helper()
	refreshTable(ctx, t, spark, fullTableName)
	df, err := spark.Sql(ctx, fmt.Sprintf("SHOW TBLPROPERTIES %s", fullTableName))
	require.NoError(t, err)
	rows, err := df.Collect(ctx)
	require.NoError(t, err)
	for _, row := range rows {
		if fmt.Sprintf("%v", row.Value("key")) == key {
			return fmt.Sprintf("%v", row.Value("value"))
		}
	}
	return ""
}

func runSpark(ctx context.Context, t *testing.T, spark sql.SparkSession, statement string) {
	t.Helper()
	df, err := spark.Sql(ctx, statement)
	require.NoError(t, err, "spark sql failed: %s", statement)
	_, err = df.Collect(ctx)
	require.NoError(t, err, "spark sql failed: %s", statement)
}
