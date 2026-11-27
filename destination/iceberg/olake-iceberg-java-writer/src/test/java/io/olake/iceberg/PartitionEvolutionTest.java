package io.olake.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * A stream's partition_regex change evolves its table's partition spec in place, and only when
 * the configured fields differ from the ones OLake last applied.
 */
class PartitionEvolutionTest {
    private static final Schema SCHEMA = new Schema(List.of(
            Types.NestedField.required(1, "_olake_id", Types.StringType.get()),
            Types.NestedField.optional(2, "region", Types.StringType.get()),
            Types.NestedField.optional(3, "created_at", Types.TimestampType.withZone()),
            Types.NestedField.optional(4, "amount", Types.LongType.get())), Set.of(1));
    private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("db"), "t");

    @TempDir
    Path dir;

    private HadoopCatalog catalog;

    @BeforeEach
    void newCatalog() {
        catalog = new HadoopCatalog(new Configuration(), dir.toString());
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({
            "identity, identity",
            "IDENTITY, identity",
            "day, day",
            "bucket[16], bucket[16]",
            "'BUCKET[ 4 ]', bucket[4]",
            "truncate[10], truncate[10]",
            "void, void",
            "bucket[x], identity", // the fallback table creation takes
            "nonsense, identity",
    })
    @DisplayName("transforms compare as Iceberg prints them")
    void canonicalTransforms(String raw, String canonical) {
        assertEquals(canonical, PartitionEvolution.canonicalTransform(raw));
    }

    @Test
    @DisplayName("a new table records the fields it was created with, and an unchanged regex changes nothing")
    void unchangedRegexIsANoOp() {
        Table table = create(regex("region", "identity"));
        assertEquals("region:identity", table.properties().get(PartitionEvolution.APPLIED_PROPERTY));

        assertFalse(PartitionEvolution.apply(table, regex("region", "identity"), true));
        assertEquals(0, table.spec().specId());
        assertEquals(1, table.specs().size());
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
            // name                 | from                              | to                                 | resulting fields
            "add a field            | region:identity                   | region:identity;created_at:day     | region:identity;created_at:day",
            "remove a field         | region:identity;created_at:day    | region:identity                    | region:identity",
            "change a transform     | created_at:day                    | created_at:month                   | created_at:month",
            "change a bucket count  | amount:bucket[4]                  | amount:bucket[8]                   | amount:bucket[8]",
            "drop all partitioning  | region:identity                   | ''                                 | ''",
            "partition a flat table | ''                                | region:identity;amount:truncate[10] | region:identity;amount:truncate[10]",
    })
    @DisplayName("a changed regex evolves the spec in place")
    void changedRegexEvolvesTheSpec(String name, String from, String to, String resulting) {
        Table table = create(parse(from));

        assertTrue(PartitionEvolution.apply(table, parse(to), true));

        assertEquals(resulting, PartitionEvolution.applied(PartitionEvolution.current(table)));
        assertEquals(to, table.properties().get(PartitionEvolution.APPLIED_PROPERTY));
        assertEquals(2, table.specs().size(), "the old spec stays, for the files written under it");
        assertFalse(PartitionEvolution.apply(table, parse(to), true), "applying the same regex again changes nothing");
    }

    @Test
    @DisplayName("a stream writing equality deletes keeps its spec")
    void equalityDeleteStreamsKeepTheirSpec() {
        Table table = create(regex("region", "identity"));

        assertFalse(PartitionEvolution.apply(table, regex("created_at", "day"), false));

        assertEquals("region:identity", PartitionEvolution.applied(PartitionEvolution.current(table)));
        assertEquals("region:identity", table.properties().get(PartitionEvolution.APPLIED_PROPERTY),
                "the record is not moved, so the change is applied once the stream can follow it");
    }

    @Test
    @DisplayName("a spec evolved by another engine is kept while the stream's regex is unchanged")
    void externalEvolutionIsKept() {
        Table table = create(regex("region", "identity"));
        table.updateSpec().addField(org.apache.iceberg.expressions.Expressions.bucket("amount", 4)).commit();

        assertFalse(PartitionEvolution.apply(table, regex("region", "identity"), true));
        assertEquals("region:identity;amount:bucket[4]", PartitionEvolution.applied(PartitionEvolution.current(table)));

        // the stream's own regex changes: OLake's configuration wins from here on
        assertTrue(PartitionEvolution.apply(table, regex("created_at", "day"), true));
        assertEquals("created_at:day", PartitionEvolution.applied(PartitionEvolution.current(table)));
    }

    @Test
    @DisplayName("a table without a record adopts a matching spec and evolves a different one")
    void tablesWithoutARecord() {
        Table matching = catalog.buildTable(TABLE, SCHEMA)
                .withPartitionSpec(org.apache.iceberg.PartitionSpec.builderFor(SCHEMA).identity("region").build()).create();
        assertFalse(PartitionEvolution.apply(matching, regex("region", "identity"), true));
        assertEquals("region:identity", matching.properties().get(PartitionEvolution.APPLIED_PROPERTY));
        assertEquals(1, matching.specs().size());

        Table different = catalog.buildTable(TableIdentifier.of(Namespace.of("db"), "t2"), SCHEMA)
                .withPartitionSpec(org.apache.iceberg.PartitionSpec.builderFor(SCHEMA).identity("region").build()).create();
        assertTrue(PartitionEvolution.apply(different, regex("created_at", "day"), true));
        assertEquals("created_at:day", PartitionEvolution.applied(PartitionEvolution.current(different)));
    }

    @Test
    @DisplayName("a regex naming a column the table does not have fails clearly")
    void unknownColumnFails() {
        Table table = create(regex("region", "identity"));
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> PartitionEvolution.apply(table, regex("missing", "identity"), true));
        assertTrue(failure.getMessage().contains("no column missing"), failure.getMessage());
    }

    // every writer thread of a stream opens the table at once: one evolves it, the rest find it evolved
    @Test
    @DisplayName("concurrent writer threads evolve the spec exactly once")
    void concurrentThreadsEvolveOnce() throws Exception {
        create(regex("region", "identity"));
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Callable<Boolean>> threads = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                threads.add(() -> PartitionEvolution.apply(catalog.loadTable(TABLE),
                        parse("region:identity;created_at:day"), true));
            }
            int evolved = 0;
            for (Future<Boolean> result : pool.invokeAll(threads)) {
                evolved += result.get() ? 1 : 0;
            }
            assertEquals(1, evolved);
        } finally {
            pool.shutdown();
        }
        Table table = catalog.loadTable(TABLE);
        assertEquals(2, table.specs().size());
        assertEquals("region:identity;created_at:day", PartitionEvolution.applied(PartitionEvolution.current(table)));
    }

    private Table create(List<Map<String, String>> regex) {
        return IcebergUtil.createIcebergTable(catalog, TABLE, SCHEMA, "parquet", regex, 2);
    }

    private static List<Map<String, String>> regex(String field, String transform) {
        return List.of(Map.of("field", field, "transform", transform));
    }

    /** {@code column:transform;...}, the form {@link PartitionEvolution#APPLIED_PROPERTY} keeps. */
    private static List<Map<String, String>> parse(String fields) {
        List<Map<String, String>> regex = new ArrayList<>();
        if (fields == null || fields.isBlank()) {
            return regex;
        }
        for (String field : fields.split(";")) {
            String[] parts = field.split(":");
            regex.add(Map.of("field", parts[0], "transform", parts[1]));
        }
        return regex;
    }
}
