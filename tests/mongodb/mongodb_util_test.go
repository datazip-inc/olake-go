package mongodb

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/performance"
	"github.com/datazip-inc/olake/tests/testutils/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	nestedDoc = bson.M{
		"nested_string":    "nested_value",
		"nested_int":       42,
		"nested_timestamp": time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
	}
)

// performanceCDCStreams is the CDC stream set the performance suite drives, shared between the
// performance.TestHandler config and the perf operations below.
var performanceCDCStreams = []string{"tweets_cdc"}

func ExecuteQuery(ctx context.Context, t *testing.T, conf *testutils.TestConfig, operation string) {
	t.Helper()

	// directConnection because the replica set advertises its member as host.docker.internal,
	// which only the driver's container resolves; the harness dials the published port directly.
	config := conf.SourceBaseConfig
	connStr := fmt.Sprintf(
		"mongodb://%s:%s@%s/?authSource=%s&readPreference=%s&directConnection=true",
		config.String("username"),
		config.String("password"),
		strings.Join(config.Hosts("hosts"), ","),
		config.String("authdb"),
		config.String("read_preference"),
	)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	require.NoError(t, err, "failed to connect to mongodb at %s", strings.Join(config.Hosts("hosts"), ","))
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			t.Logf("warning: failed to disconnect from MongoDB: %v", err)
		}
	}()

	integrationTestCollection := conf.GetTableName()
	db := client.Database(config.String("database"))
	collection := db.Collection(integrationTestCollection)

	switch operation {
	case "create":
		// Create collection by inserting a dummy document and then deleting it
		dummyDoc := bson.M{"_dummy": "create_collection"}
		_, err := collection.InsertOne(ctx, dummyDoc)
		require.NoError(t, err, "Failed to create collection")
		_, err = collection.DeleteOne(ctx, bson.M{"_dummy": "create_collection"})
		require.NoError(t, err, "Failed to clean up dummy document")

	case "drop":
		err := collection.Drop(ctx)
		require.NoError(t, err, "Failed to drop collection")

	case "drop-all":
		require.NoError(t, db.Drop(ctx), "Failed to drop database")

	case "clean":
		_, err := collection.DeleteMany(ctx, bson.M{})
		require.NoError(t, err, "Failed to clean collection")

	case "add":
		insertTestData(ctx, t, collection)
		return

	case "add_mixed_id_mostly_objectid", "add_mixed_id_mostly_integer":
		mixedCase, _ := mixedIDCaseBySeedOp(operation)
		insertMixedIDData(ctx, t, collection, mixedCase.docs())
		return

	case "insert":
		// Insert the same data as the add operation
		doc := bson.M{
			"id_bigint":             int64(123456789012345),
			"id_int":                int32(100),
			"id_timestamp":          time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			"id_double":             float64(123.456),
			"id_bool":               true,
			"id_cursor":             int32(6),
			"created_timestamp":     primitive.Timestamp{T: uint32(1754905992), I: 1},
			"id_nil":                nil,
			"id_regex":              primitive.Regex{Pattern: "test.*", Options: "i"},
			"id_nested":             nestedDoc,
			"id_minkey":             primitive.MinKey{},
			"id_maxkey":             primitive.MaxKey{},
			"name_varchar":          "varchar_val",
			"id_int32_edge":         int32(-2147483648),
			"id_int64_edge":         int64(-9223372036854775808),
			"id_double_max":         float64(1.7976931348623157e308),
			"id_double_tiny":        float64(5e-324),
			"id_double_negzero":     math.Copysign(0, -1),
			"id_string_empty":       "",
			"id_string_special":     "line1\nline2\ttab \"q\" 's' \\ back 世界🎉",
			"id_string_long":        strings.Repeat("x", 100000),
			"id_date_edge":          time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			"id_date_preepoch":      time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
			"id_val_to_null":        int32(5),
			"id_val_to_null_string": "x",
			"id_val_to_null_double": float64(2.5),
			"id_val_to_null_date":   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			"id_null_stay":          nil,
			"id_empty_doc":          bson.M{},
			"id_nested_deep":        bson.M{"a": bson.M{"b": bson.M{"c": bson.M{"d": "deep"}}}},
			"excludedColumn":        101,
		}
		_, err := collection.InsertOne(ctx, doc)
		require.NoError(t, err, "Failed to insert document")
		// insert a filtered doc, it would be filtered out by the filter, won't be synced into the destination
		filteredDoc := bson.M{
			"id":             999,
			"id_cursor":      -1,
			"id_bigint":      int64(111111111111111),
			"id_int":         int32(0),
			"id_timestamp":   time.Date(2022, 6, 15, 10, 0, 0, 0, time.UTC),
			"id_double":      float64(50.123),
			"excludedColumn": 200,
		}
		_, err = collection.InsertOne(ctx, filteredDoc)
		require.NoError(t, err, "Failed to insert filtered test data row")

	case "insert_2pc":
		doc2 := bson.M{
			"id_bigint":             int64(123456789012345),
			"id_int":                int32(100),
			"id_timestamp":          time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			"id_double":             float64(123.456),
			"id_bool":               true,
			"id_cursor":             int32(7),
			"created_timestamp":     primitive.Timestamp{T: uint32(1754905992), I: 1},
			"id_nil":                nil,
			"id_regex":              primitive.Regex{Pattern: "test.*", Options: "i"},
			"id_nested":             nestedDoc,
			"id_minkey":             primitive.MinKey{},
			"id_maxkey":             primitive.MaxKey{},
			"name_varchar":          "varchar_val",
			"id_int32_edge":         int32(-2147483648),
			"id_int64_edge":         int64(-9223372036854775808),
			"id_double_max":         float64(1.7976931348623157e308),
			"id_double_tiny":        float64(5e-324),
			"id_double_negzero":     math.Copysign(0, -1),
			"id_string_empty":       "",
			"id_string_special":     "line1\nline2\ttab \"q\" 's' \\ back 世界🎉",
			"id_string_long":        strings.Repeat("x", 100000),
			"id_date_edge":          time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			"id_date_preepoch":      time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
			"id_val_to_null":        int32(5),
			"id_val_to_null_string": "x",
			"id_val_to_null_double": float64(2.5),
			"id_val_to_null_date":   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			"id_null_stay":          nil,
			"id_empty_doc":          bson.M{},
			"id_nested_deep":        bson.M{"a": bson.M{"b": bson.M{"c": bson.M{"d": "deep"}}}},
		}
		_, err2 := collection.InsertOne(ctx, doc2)
		require.NoError(t, err2, "Failed to insert document (insert_2pc)")

	case "update":
		filter := bson.M{"id": int32(1)}
		update := bson.M{
			"$set": bson.M{
				"id_bigint":             int64(987654321098765),
				"id_int":                int64(200),
				"id_timestamp":          time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC),
				"id_double":             float64(202.456),
				"id_bool":               false,
				"id_cursor":             nil,
				"created_timestamp":     primitive.Timestamp{T: uint32(1754905699), I: 1},
				"id_nil":                nil,
				"id_regex":              primitive.Regex{Pattern: "updated.*", Options: "i"},
				"id_nested":             nestedDoc,
				"id_minkey":             primitive.MinKey{},
				"id_maxkey":             primitive.MaxKey{},
				"name_varchar":          "updated varchar",
				"id_int32_edge":         int32(2147483647),
				"id_int64_edge":         int64(9223372036854775807),
				"id_double_max":         float64(-1.7976931348623157e308),
				"id_double_tiny":        float64(-5e-324),
				"id_double_negzero":     float64(0),
				"id_string_empty":       nil,
				"id_string_special":     "",
				"id_string_long":        "short",
				"id_date_edge":          time.Date(9999, 12, 31, 23, 59, 59, 999000000, time.UTC),
				"id_date_preepoch":      time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC),
				"id_val_to_null":        nil,
				"id_val_to_null_string": nil,
				"id_val_to_null_double": nil,
				"id_val_to_null_date":   nil,
				"id_null_stay":          nil,
				"id_empty_doc":          bson.M{"k": nil, "arr": bson.A{int32(1), int32(2)}},
				"id_nested_deep":        bson.M{},
				"excludedColumn":        102,
				"includedColumn":        int32(202),
			},
		}
		_, err := collection.UpdateOne(ctx, filter, update)
		require.NoError(t, err, "Failed to update document")

	case "delete":
		filter := bson.M{"id": 1}
		_, err := collection.DeleteOne(ctx, filter)
		require.NoError(t, err, "Failed to delete document")

	case "setup_cdc":
		// truncate the cdc tables
		for _, cdcStream := range performanceCDCStreams {
			_, err := client.Database(config.String("database")).Collection(cdcStream).DeleteMany(ctx, bson.D{})
			require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
		}
		return

	case "bulk_cdc_data_insert":
		backfillStreams := performance.GetBackfillStreamsFromCDC(performanceCDCStreams)
		totalRows := 15000000

		// TODO: insert data in batch
		// insert the data into the cdc tables concurrently
		err := testutils.Concurrent(ctx, performanceCDCStreams, len(performanceCDCStreams), func(ctx context.Context, cdcStream string, executionNumber int) error {
			srcColl := client.Database(config.String("database")).Collection(backfillStreams[executionNumber])
			destColl := client.Database(config.String("database")).Collection(cdcStream)

			cursor, err := srcColl.Find(ctx, bson.D{}, options.Find().SetLimit(int64(totalRows)))
			if err != nil {
				return fmt.Errorf("stream: %s, error: %s", cdcStream, err)
			}
			defer cursor.Close(ctx)

			var docs []interface{}
			for cursor.Next(ctx) {
				var doc bson.M
				if err := cursor.Decode(&doc); err != nil {
					return err
				}
				docs = append(docs, doc)
			}
			if err := cursor.Err(); err != nil {
				return err
			}
			if len(docs) == 0 {
				return nil
			}
			_, err = destColl.InsertMany(ctx, docs)
			if err != nil {
				return fmt.Errorf("stream: %s, error: %s", cdcStream, err)
			}
			return nil
		})
		require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
		return
	}
}

// seedDocument is the document every seeded row starts from; callers set the per-row fields, and
// seedColumnTypes reads the types off it.
func seedDocument() bson.M {
	return bson.M{
		"id_bigint":             int64(123456789012345),
		"id_int":                int32(100),
		"id_timestamp":          time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		"id_double":             float64(123.456),
		"id_bool":               true,
		"created_timestamp":     primitive.Timestamp{T: uint32(1754905992), I: 1},
		"id_nil":                nil,
		"id_regex":              primitive.Regex{Pattern: "test.*", Options: "i"},
		"id_nested":             nestedDoc,
		"id_minkey":             primitive.MinKey{},
		"id_maxkey":             primitive.MaxKey{},
		"name_varchar":          "varchar_val",
		"id_int32_edge":         int32(-2147483648),
		"id_int64_edge":         int64(-9223372036854775808),
		"id_double_max":         float64(1.7976931348623157e308),
		"id_double_tiny":        float64(5e-324),
		"id_double_negzero":     math.Copysign(0, -1),
		"id_string_empty":       "",
		"id_string_special":     "line1\nline2\ttab \"q\" 's' \\ back 世界🎉",
		"id_string_long":        strings.Repeat("x", 100000),
		"id_date_edge":          time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		"id_date_preepoch":      time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
		"id_val_to_null":        int32(5),
		"id_val_to_null_string": "x",
		"id_val_to_null_double": float64(2.5),
		"id_val_to_null_date":   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		"id_null_stay":          nil,
		"id_empty_doc":          bson.M{},
		"id_nested_deep":        bson.M{"a": bson.M{"b": bson.M{"c": bson.M{"d": "deep"}}}},
		"excludedColumn":        100,
	}
}

// seedColumnTypes tags every seed field with its BSON type name (regex, date, ...), read off the
// seed document itself, so a data_types rule in compatibility_rules.json follows a seed edit with
// nothing to declare.
func seedColumnTypes() map[string][]string {
	types := map[string][]string{}
	for field, value := range seedDocument() {
		bsonType, _, err := bson.MarshalValue(value)
		if err != nil {
			continue
		}
		types[field] = []string{strings.ToLower(bsonType.String())}
	}
	return types
}

func insertTestData(ctx context.Context, t *testing.T, collection *mongo.Collection) {
	t.Helper()
	for i := 1; i <= 5; i++ {
		doc := seedDocument()
		doc["id"] = i
		doc["id_cursor"] = i

		_, err := collection.InsertOne(ctx, doc)
		require.NoError(t, err, "Failed to insert test data row %d", i)
	}
	// insert a filtered doc, it would be filtered out by the filter, won't be synced into the destination
	filteredDoc := bson.M{
		"id":             999,
		"id_cursor":      -1,
		"id_bigint":      int64(111111111111111),
		"id_int":         int32(0),
		"id_timestamp":   time.Date(2021, 6, 15, 10, 0, 0, 0, time.UTC),
		"id_double":      float64(500234.123),
		"excludedColumn": 200,
	}
	_, err := collection.InsertOne(ctx, filteredDoc)
	require.NoError(t, err, "Failed to insert filtered test data row")
}

var ExpectedMongoData = map[string]interface{}{
	"id_bigint":             int64(123456789012345),
	"id_int":                int32(100),
	"id_timestamp":          arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"id_double":             float64(123.456),
	"id_bool":               true,
	"created_timestamp":     int32(1754905992),
	"id_regex":              `{"Pattern":"test.*","Options":"i"}`,
	"id_nested":             `{"nested_int":42,"nested_string":"nested_value","nested_timestamp":"2023-01-01T12:00:00Z"}`,
	"id_minkey":             `{}`,
	"id_maxkey":             `{}`,
	"name_varchar":          "varchar_val",
	"id_int32_edge":         int32(-2147483648),
	"id_int64_edge":         int64(-9223372036854775808),
	"id_double_max":         float64(1.7976931348623157e308),
	"id_double_tiny":        float64(5e-324),
	"id_double_negzero":     float64(0),
	"id_string_empty":       "",
	"id_string_special":     "line1\nline2\ttab \"q\" 's' \\ back 世界🎉",
	"id_string_long":        strings.Repeat("x", 100000),
	"id_date_edge":          arrow.Timestamp(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()),
	"id_date_preepoch":      arrow.Timestamp(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()),
	"id_val_to_null":        int32(5),
	"id_val_to_null_string": "x",
	"id_val_to_null_double": float64(2.5),
	"id_val_to_null_date":   arrow.Timestamp(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()),
	"id_null_stay":          nil,
	"id_empty_doc":          `{}`,
	"id_nested_deep":        `{"a":{"b":{"c":{"d":"deep"}}}}`,
}

var ExpectedUpdatedData = map[string]interface{}{
	"id_bigint":             int64(987654321098765),
	"id_int":                int64(200),
	"id_timestamp":          arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"id_double":             float64(202.456),
	"id_bool":               false,
	"created_timestamp":     int32(1754905699),
	"id_regex":              `{"Pattern":"updated.*","Options":"i"}`,
	"id_nested":             `{"nested_int":42,"nested_string":"nested_value","nested_timestamp":"2023-01-01T12:00:00Z"}`,
	"id_minkey":             `{}`,
	"id_maxkey":             `{}`,
	"name_varchar":          "updated varchar",
	"includedcolumn":        int32(202),
	"id_int32_edge":         int32(2147483647),
	"id_int64_edge":         int64(9223372036854775807),
	"id_double_max":         float64(-1.7976931348623157e308),
	"id_double_tiny":        float64(-5e-324),
	"id_double_negzero":     float64(0),
	"id_string_empty":       nil,
	"id_string_special":     "",
	"id_string_long":        "short",
	"id_date_edge":          arrow.Timestamp(time.Date(9999, 12, 31, 23, 59, 59, 999000000, time.UTC).UnixMicro()),
	"id_date_preepoch":      arrow.Timestamp(time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC).UnixMicro()),
	"id_val_to_null":        nil,
	"id_val_to_null_string": nil,
	"id_val_to_null_double": nil,
	"id_val_to_null_date":   nil,
	"id_null_stay":          nil,
	"id_empty_doc":          `{"arr":[1,2],"k":null}`,
	"id_nested_deep":        `{}`,
}

var MongoToDestinationSchema = map[string]string{
	"id_bigint":             "bigint",
	"id_int":                "int",
	"id_timestamp":          "timestamp",
	"id_double":             "double",
	"id_bool":               "boolean",
	"created_timestamp":     "int",
	"id_nil":                "null",
	"id_regex":              "string",
	"id_nested":             "string",
	"id_minkey":             "string",
	"id_maxkey":             "string",
	"name_varchar":          "string",
	"id_int32_edge":         "int",
	"id_int64_edge":         "bigint",
	"id_double_max":         "double",
	"id_double_tiny":        "double",
	"id_double_negzero":     "double",
	"id_string_empty":       "string",
	"id_string_special":     "string",
	"id_string_long":        "string",
	"id_date_edge":          "timestamp",
	"id_date_preepoch":      "timestamp",
	"id_val_to_null":        "int",
	"id_val_to_null_string": "string",
	"id_val_to_null_double": "double",
	"id_val_to_null_date":   "timestamp",
	"id_null_stay":          "null",
	"id_empty_doc":          "string",
	"id_nested_deep":        "string",
}

var UpdatedMongoToDestinationSchema = map[string]string{
	"id_bigint":             "bigint",
	"id_int":                "bigint",
	"id_timestamp":          "timestamp",
	"id_double":             "double",
	"id_bool":               "boolean",
	"created_timestamp":     "int",
	"id_nil":                "null",
	"id_regex":              "string",
	"id_nested":             "string",
	"id_minkey":             "string",
	"id_maxkey":             "string",
	"name_varchar":          "string",
	"includedcolumn":        "int",
	"id_int32_edge":         "int",
	"id_int64_edge":         "bigint",
	"id_double_max":         "double",
	"id_double_tiny":        "double",
	"id_double_negzero":     "double",
	"id_string_empty":       "string",
	"id_string_special":     "string",
	"id_string_long":        "string",
	"id_date_edge":          "timestamp",
	"id_date_preepoch":      "timestamp",
	"id_val_to_null":        "int",
	"id_val_to_null_string": "string",
	"id_val_to_null_double": "double",
	"id_val_to_null_date":   "timestamp",
	"id_null_stay":          "null",
	"id_empty_doc":          "string",
	"id_nested_deep":        "string",
}

var ExpectedMongoDBDefaultCDCColumnsSchema = map[string]string{
	"_cdc_resume_token": "string",
	"_cdc_timestamp":    "timestamp",
}

// MongoTypeMapping returns a new map from the source types MongoToDestinationSchema declare to the
// type they land as in the destination. A declared type missing here fails the suite with "No
// mapping defined".
func MongoTypeMapping() map[string]string {
	return map[string]string{
		"bigint":    "bigint",
		"boolean":   "boolean",
		"double":    "double",
		"int":       "int",
		"null":      "string",
		"string":    "string",
		"timestamp": "timestamp",
	}
}

// MongoParquetTypeMapping is MongoTypeMapping for Parquet, which writes an always-null field as untyped bytes that Spark reads as binary.
// NOTE: this behavior was even before the binary column support was added in olake
func MongoParquetTypeMapping() map[string]string {
	mapping := MongoTypeMapping()
	mapping["null"] = "binary"
	return mapping
}
