package io.olake.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * IcebergUtil.evolvePartitionSpec and the spec comparison it relies on.
 */
class PartitionEvolutionTest {
    private static final Schema SCHEMA = new Schema(List.of(
            Types.NestedField.required(1, "_olake_id", Types.StringType.get()),
            Types.NestedField.optional(2, "id", Types.LongType.get()),
            Types.NestedField.optional(3, "created_at", Types.TimestampType.withZone())), Set.of(1));

    @TempDir
    Path dir;

    private Table createTable(PartitionSpec spec) {
        return new HadoopTables(new Configuration()).create(SCHEMA, spec, Map.of("format-version", "2"),
                dir.resolve("t").toString());
    }

    private static List<Map<String, String>> fields(String... fieldTransformPairs) {
        List<Map<String, String>> out = new ArrayList<>();
        for (int i = 0; i < fieldTransformPairs.length; i += 2) {
            out.add(Map.of("field", fieldTransformPairs[i], "transform", fieldTransformPairs[i + 1]));
        }
        return out;
    }

    @Test
    void matchingSpecIsANoOp() {
        Table table = createTable(PartitionSpec.builderFor(SCHEMA).day("created_at").bucket("id", 8).build());

        assertFalse(IcebergUtil.evolvePartitionSpec(table, SCHEMA, fields("id", "BUCKET[ 8 ]", "created_at", "day")));
        assertEquals(0, table.spec().specId());
    }

    @Test
    void changesTransformAndAddsField() {
        Table table = createTable(PartitionSpec.builderFor(SCHEMA).day("created_at").build());
        List<Map<String, String>> desired = fields("created_at", "month", "id", "bucket[8]");

        assertTrue(IcebergUtil.evolvePartitionSpec(table, SCHEMA, desired));

        assertEquals(1, table.spec().specId());
        assertEquals(2, table.specs().size(), "the old spec stays in the table's history");
        assertTrue(IcebergUtil.specMatches(table.spec(), table.schema(), desired));
        // a second writer thread asking for the same spec finds it already evolved
        assertFalse(IcebergUtil.evolvePartitionSpec(table, SCHEMA, desired));
        assertEquals(1, table.spec().specId());
    }

    @Test
    void changesBucketCount() {
        Table table = createTable(PartitionSpec.builderFor(SCHEMA).bucket("id", 16).build());
        List<Map<String, String>> desired = fields("id", "bucket[32]");

        assertTrue(IcebergUtil.evolvePartitionSpec(table, SCHEMA, desired));

        assertEquals(List.of(Map.of("field", "id", "transform", "bucket[32]")),
                IcebergUtil.partitionFields(table.spec(), table.schema()));
    }

    @Test
    void removesEveryField() {
        Table table = createTable(PartitionSpec.builderFor(SCHEMA).day("created_at").identity("id").build());

        assertTrue(IcebergUtil.evolvePartitionSpec(table, SCHEMA, fields()));

        assertTrue(table.spec().isUnpartitioned());
    }

    @Test
    void addsMissingPartitionColumnFromRequestedSchema() {
        Table table = createTable(PartitionSpec.unpartitioned());
        Schema requested = new Schema(List.of(
                Types.NestedField.required(1, "_olake_id", Types.StringType.get()),
                Types.NestedField.optional(2, "id", Types.LongType.get()),
                Types.NestedField.optional(3, "created_at", Types.TimestampType.withZone()),
                Types.NestedField.optional(4, "region", Types.StringType.get())), Set.of(1));

        assertTrue(IcebergUtil.evolvePartitionSpec(table, requested, fields("region", "identity")));

        assertNotNull(table.schema().findField("region"));
        assertEquals(List.of(Map.of("field", "region", "transform", "identity")),
                IcebergUtil.partitionFields(table.spec(), table.schema()));
    }

    @Test
    void refusesTableWithDeleteFiles() {
        Table table = createTable(PartitionSpec.unpartitioned());
        table.newRowDelta().addDeletes(FileMetadata.deleteFileBuilder(table.spec())
                .ofPositionDeletes()
                .withPath(dir.resolve("deletes.parquet").toString())
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build()).commit();

        assertThrows(UnsupportedOperationException.class,
                () -> IcebergUtil.evolvePartitionSpec(table, SCHEMA, fields("id", "identity")));
        assertTrue(table.spec().isUnpartitioned());
    }

    @Test
    void specMatchesIgnoresOrder() {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", 8).month("created_at").build();

        assertTrue(IcebergUtil.specMatches(spec, SCHEMA, fields("created_at", "month", "id", "bucket[8]")));
        assertFalse(IcebergUtil.specMatches(spec, SCHEMA, fields("created_at", "day", "id", "bucket[8]")));
        assertFalse(IcebergUtil.specMatches(spec, SCHEMA, fields("created_at", "month")));
    }

    @Test
    void rejectsInvalidTransform() {
        assertThrows(IllegalArgumentException.class, () -> IcebergUtil.toTerm("id", "bucket[x]"));
        assertThrows(IllegalArgumentException.class, () -> IcebergUtil.toTerm("id", "bucket[0]"));
        assertThrows(IllegalArgumentException.class, () -> IcebergUtil.toTerm("id", "weekly"));
    }
}
