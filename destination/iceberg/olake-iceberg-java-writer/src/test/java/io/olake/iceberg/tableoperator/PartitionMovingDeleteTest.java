package io.olake.iceberg.tableoperator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.olake.iceberg.IcebergUtil;
import io.olake.iceberg.rpc.RecordIngest.ArrowPayload;
import io.olake.iceberg.tableIndex.EqualityDeleteMigrator;

/**
 * An update that moves a row to another partition supersedes a row in a data file of the
 * OLD partition. Iceberg scopes deletes to a partition, so the delete has to carry the old
 * file's partition: stamped with the new row's, readers that filter on the partition column
 * still see the old row, a delete file spanning several data files misses it on every read,
 * and Iceberg 1.12+ refuses the table. The same holds for the partition SPEC of a file written
 * before the table's spec was evolved.
 */
class PartitionMovingDeleteTest {
    private static final Schema SCHEMA = new Schema(List.of(
            Types.NestedField.required(1, "_olake_id", Types.StringType.get()),
            Types.NestedField.optional(2, "payload", Types.StringType.get()),
            Types.NestedField.optional(3, "region", Types.StringType.get())), Set.of(1));
    private static final PartitionSpec BY_REGION = PartitionSpec.builderFor(SCHEMA).identity("region").build();

    @TempDir
    Path dir;

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = DeleteMode.class, names = {"POSITION", "DELETION_VECTOR"})
    @DisplayName("a row moved to another partition is deleted in the partition it moved out of")
    void deletesTheMovedRowInItsOldPartition(DeleteMode mode) throws Exception {
        Table table = newTable(mode);
        String euFile = commitRows(table, mode, row("1", "a", "eu"), row("2", "b", "eu"));

        PositionalDeltaWriter writer = newWriter(table, mode, new DataFilePartitions(table));
        writer.write(update("1", "a2", "us", euFile, 0));
        WriteResult result = writer.complete();

        assertEquals(List.of("region=eu"), partitions(table, result.deleteFiles()));
        commit(table, result);
        assertEquals(List.of("1/us", "2/eu"), read(table, Expressions.alwaysTrue()));
        assertEquals(List.of("2/eu"), read(table, Expressions.equal("region", "eu")));
    }

    // the row index hands out a location in a file this session wrote but has not committed:
    // no manifest lists it, so its partition has to come from the writer itself
    @ParameterizedTest(name = "{0}")
    @EnumSource(value = DeleteMode.class, names = {"POSITION", "DELETION_VECTOR"})
    @DisplayName("a row moved out of a file this session has not committed yet is deleted in that file's partition")
    void deletesInAnUncommittedFilesPartition(DeleteMode mode) throws Exception {
        Table table = newTable(mode);
        PositionalDeltaWriter writer = newWriter(table, mode, new DataFilePartitions(table));
        Record first = row("1", "a", "eu");
        String openFile = writer.currentPath(first).toString();
        writer.write(first);
        writer.batchCompleted(); // the next batch reaches the row through the index, not the writer's own map

        writer.write(update("1", "a2", "us", openFile, 0));
        WriteResult result = writer.complete();

        assertEquals(List.of("region=eu"), partitions(table, result.deleteFiles()));
        commit(table, result);
        assertEquals(List.of("1/us"), read(table, Expressions.alwaysTrue()));
        assertEquals(List.of(), read(table, Expressions.equal("region", "eu")));
    }

    // a manifest's partition and the writer's own partition key are different classes; deletes
    // for the same partition still have to share one delete file
    @Test
    @DisplayName("deletes resolved from a manifest and from the session share one delete file per partition")
    void oneDeleteFilePerPartition() throws Exception {
        Table table = newTable(DeleteMode.POSITION);
        String committed = commitRows(table, DeleteMode.POSITION, row("1", "a", "eu"));

        PositionalDeltaWriter writer = newWriter(table, DeleteMode.POSITION, new DataFilePartitions(table));
        Record uncommittedRow = row("3", "c", "eu");
        String uncommitted = writer.currentPath(uncommittedRow).toString();
        writer.write(uncommittedRow);
        writer.batchCompleted();

        writer.write(update("1", "a2", "us", committed, 0));
        writer.write(update("3", "c2", "us", uncommitted, 0));
        WriteResult result = writer.complete();

        assertEquals(List.of("region=eu"), partitions(table, result.deleteFiles()));
        commit(table, result);
        assertEquals(List.of("1/us", "3/us"), read(table, Expressions.alwaysTrue()));
    }

    // a spec evolved outside OLake: rows still in files of the old spec are deleted under that
    // spec, whether the update moves them (1) or not (2)
    @ParameterizedTest(name = "{0}")
    @EnumSource(value = DeleteMode.class, names = {"POSITION", "DELETION_VECTOR"})
    @DisplayName("a row in a file of an older partition spec is deleted under that file's spec")
    void deletesUnderTheOlderSpecOfTheFile(DeleteMode mode) throws Exception {
        Table table = newTable(mode);
        String euFile = commitRows(table, mode, row("1", "a", "eu"), row("2", "b", "eu"));
        int oldSpec = table.spec().specId();
        table.updateSpec().addField(Expressions.bucket("_olake_id", 2)).commit();

        PositionalDeltaWriter writer = newWriter(table, mode, new DataFilePartitions(table));
        writer.write(update("1", "a2", "us", euFile, 0));
        writer.write(update("2", "b2", "eu", euFile, 1));
        WriteResult result = writer.complete();

        assertEquals(List.of(oldSpec + ":region=eu"), specsAndPartitions(table, result.deleteFiles()));
        commit(table, result);
        assertEquals(List.of("1/us", "2/eu"), read(table, Expressions.alwaysTrue()));
        assertEquals(List.of("2/eu"), read(table, Expressions.equal("region", "eu")));
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = DeleteMode.class, names = {"POSITION", "DELETION_VECTOR"})
    @DisplayName("a table evolved to unpartitioned still deletes into its partitioned files under their spec")
    void deletesIntoPartitionedFilesOfATableNowUnpartitioned(DeleteMode mode) throws Exception {
        Table table = newTable(mode);
        String euFile = commitRows(table, mode, row("1", "a", "eu"), row("2", "b", "eu"));
        int oldSpec = table.spec().specId();
        table.updateSpec().removeField("region").commit();

        PositionalDeltaWriter writer = newWriter(table, mode, new DataFilePartitions(table));
        writer.write(update("1", "a2", "us", euFile, 0));
        WriteResult result = writer.complete();

        assertEquals(List.of(oldSpec + ":region=eu"), specsAndPartitions(table, result.deleteFiles()));
        commit(table, result);
        assertEquals(List.of("1/us", "2/eu"), read(table, Expressions.alwaysTrue()));
        assertEquals(List.of("2/eu"), read(table, Expressions.equal("region", "eu")));
    }

    // eq -> pos/dv migration of a table whose spec was evolved after the equality delete's data file
    @ParameterizedTest(name = "{0}")
    @EnumSource(value = DeleteMode.class, names = {"POSITION", "DELETION_VECTOR"})
    @DisplayName("migrating equality deletes keeps each data file's own spec")
    void migratesEqualityDeletesUnderTheFilesSpec(DeleteMode mode) throws Exception {
        Table table = newTable(mode);
        String euFile = commitRows(table, mode, row("1", "a", "eu"), row("2", "b", "eu"));
        PartitionSpec oldSpec = table.spec();
        StructLike eu = partitionOf(table, euFile);
        table.newRowDelta().addDeletes(equalityDelete(table, oldSpec, eu, "1")).commit();
        table.updateSpec().addField(Expressions.bucket("_olake_id", 2)).commit();

        EqualityDeleteMigrator.migrate(table, "_olake_id",
                IcebergUtil.getTableOutputFileFactory(table, FileFormat.PARQUET), mode);

        table.refresh();
        List<DeleteFile> deletes = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            tasks.forEach(task -> deletes.addAll(task.deletes()));
        }
        assertEquals(List.of(oldSpec.specId() + ":region=eu"), specsAndPartitions(table, deletes.toArray(DeleteFile[]::new)));
        assertEquals(FileContent.POSITION_DELETES, deletes.get(0).content());
        assertEquals(List.of("2/eu"), read(table, Expressions.alwaysTrue()));
        assertEquals(List.of("2/eu"), read(table, Expressions.equal("region", "eu")));
    }

    // the arrow path: the caller streams positions with the partition it is writing; the
    // server stamps a committed file's own partition, and keeps the caller's for a file no
    // manifest lists yet (one the caller uploaded this sync)
    @Test
    @DisplayName("arrow deletion vectors take a committed file's partition from its manifest")
    void arrowVectorsUseTheCommittedFilesPartition() throws Exception {
        Table table = newTable(DeleteMode.DELETION_VECTOR);
        String euFile = commitRows(table, DeleteMode.DELETION_VECTOR, row("1", "a", "eu"));
        String uploadedThisSync = table.location() + "/data/region=jp/uploaded.parquet";
        DataFilePartitions partitions = new DataFilePartitions(table);
        partitions.recordUncommitted(uploadedThisSync);

        ArrowDeletionVectorWriter vectors = new ArrowDeletionVectorWriter(table, partitions);
        vectors.add(ArrowPayload.DeletionVectorBatch.newBuilder()
                .addEntries(entry(euFile, "us"))
                .addEntries(entry(uploadedThisSync, "jp"))
                .build(), new IcebergTableOperator(true, DeleteMode.DELETION_VECTOR, "_olake_id"));
        DeleteWriteResult result = vectors.complete();

        Map<String, String> partitionByFile = result.deleteFiles().stream().collect(Collectors.toMap(
                DeleteFile::referencedDataFile, file -> table.spec().partitionToPath(file.partition())));
        assertEquals(Map.of(euFile, "region=eu", uploadedThisSync, "region=jp"), partitionByFile);
    }

    private Table newTable(DeleteMode mode) {
        String formatVersion = mode == DeleteMode.DELETION_VECTOR ? "3" : "2";
        return new HadoopTables(new Configuration()).create(SCHEMA, BY_REGION, Map.of("format-version", formatVersion),
                dir.resolve(mode.name()).toString());
    }

    /** A writer built the way the table operator builds one. */
    private static PositionalDeltaWriter newWriter(Table table, DeleteMode mode, DataFilePartitions partitions) {
        IcebergTableWriterFactory factory = new IcebergTableWriterFactory();
        factory.deleteMode = mode;
        factory.identifierField = "_olake_id";
        factory.dataFilePartitions = partitions;
        return (PositionalDeltaWriter) factory.create(table);
    }

    /** Commits rows of one partition in their own data file and returns its path. */
    private static String commitRows(Table table, DeleteMode mode, Record... rows) throws Exception {
        PositionalDeltaWriter writer = newWriter(table, mode, new DataFilePartitions(table));
        for (Record row : rows) {
            writer.write(row);
        }
        WriteResult result = writer.complete();
        AppendFiles append = table.newAppend();
        Arrays.stream(result.dataFiles()).forEach(append::appendFile);
        append.commit();
        assertEquals(1, result.dataFiles().length);
        return result.dataFiles()[0].location();
    }

    private static void commit(Table table, WriteResult result) {
        RowDelta delta = table.newRowDelta();
        Arrays.stream(result.dataFiles()).forEach(delta::addRows);
        Arrays.stream(result.deleteFiles()).forEach(delta::addDeletes);
        Arrays.stream(result.rewrittenDeleteFiles()).forEach(delta::removeDeletes);
        delta.commit();
    }

    private static List<String> specsAndPartitions(Table table, DeleteFile[] deletes) {
        return Arrays.stream(deletes)
                .map(file -> file.specId() + ":" + table.specs().get(file.specId()).partitionToPath(file.partition()))
                .distinct().sorted().toList();
    }

    private static StructLike partitionOf(Table table, String path) throws Exception {
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (task.file().location().equals(path)) {
                    return task.file().copyWithoutStats().partition();
                }
            }
        }
        throw new IllegalStateException("not in table: " + path);
    }

    private static DeleteFile equalityDelete(Table table, PartitionSpec spec, StructLike partition, String id) throws Exception {
        Schema keySchema = table.schema().select("_olake_id");
        EqualityDeleteWriter<Record> writer = IcebergUtil.getTableAppender(table, spec).newEqDeleteWriter(
                IcebergUtil.getTableOutputFileFactory(table, FileFormat.PARQUET).newOutputFile(spec, partition),
                FileFormat.PARQUET, partition);
        GenericRecord key = GenericRecord.create(keySchema);
        key.setField("_olake_id", id);
        try (writer) {
            writer.write(key);
        }
        return writer.toDeleteFile();
    }

    private static List<String> partitions(Table table, DeleteFile[] deletes) {
        return Arrays.stream(deletes).map(file -> table.spec().partitionToPath(file.partition())).sorted().toList();
    }

    private static List<String> read(Table table, Expression filter) throws Exception {
        table.refresh();
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(filter).build()) {
            for (Record record : records) {
                rows.add(record.getField("_olake_id") + "/" + record.getField("region"));
            }
        }
        Collections.sort(rows);
        return rows;
    }

    private static ArrowPayload.DeletionVectorBatch.Entry entry(String path, String region) {
        return ArrowPayload.DeletionVectorBatch.Entry.newBuilder()
                .setDataFilePath(path)
                .addPositions(0)
                .addPartitionValues(ArrowPayload.FileMetadata.PartitionValue.newBuilder().setStringValue(region))
                .build();
    }

    private static Record row(String id, String payload, String region) {
        return new RecordWrapper(record(id, payload, region), Operation.CREATE);
    }

    /** An update whose previous version the row index found at (path, position). */
    private static Record update(String id, String payload, String region, String path, long position) {
        return new RecordWrapper(record(id, payload, region), Operation.UPDATE, path, position);
    }

    private static GenericRecord record(String id, String payload, String region) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("_olake_id", id);
        record.setField("payload", payload);
        record.setField("region", region);
        return record;
    }
}
