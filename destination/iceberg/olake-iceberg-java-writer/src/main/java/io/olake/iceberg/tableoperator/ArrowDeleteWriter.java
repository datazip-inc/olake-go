package io.olake.iceberg.tableoperator;

import java.io.IOException;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.io.DeleteWriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.olake.iceberg.IcebergUtil;
import io.olake.iceberg.rpc.RecordIngest.ArrowPayload;
import io.olake.iceberg.tableIndex.PreviousDeleteLoader;

/**
 * Turns the superseded row positions the arrow writer streams over gRPC into delete files:
 * Iceberg v3 deletion vectors under {@link DeleteMode#DELETION_VECTOR}, Parquet positional
 * delete files under {@link DeleteMode#POSITION}.
 *
 * <p>The caller sends {@code (data file, positions)} - the form the data already has on its
 * side - and writes no delete file of its own. That keeps the one decision it cannot make
 * here: which spec and partition each delete is stamped with. That is the spec and partition
 * of the data file the delete REFERENCES, which only the table's manifests know for a file
 * committed by an earlier sync (see {@link DataFilePartitions}).
 *
 * <p>Nothing is written to storage until {@link #complete()}: positions accumulate in roaring
 * bitmaps ({@code BaseDVFileWriter}) or sorting buffers ({@code SortingPositionOnlyDeleteWriter}),
 * so a sync that never commits simply drops this object. Closing an abandoned writer would
 * produce the orphan files it looks like it is preventing.
 *
 * <p>The partition values the caller sends are read against {@code table.spec()}, in
 * partition_regex order (see {@link IcebergTableOperator#partitionDataFromTypedValues}). They
 * are only used for a file the caller uploaded this sync, which no manifest lists yet.
 *
 * <p>One instance per session, created on the first batch. Not thread-safe; a session
 * is driven by a single caller thread, like every other writer-scoped object here.
 */
public final class ArrowDeleteWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowDeleteWriter.class);

  private final Table table;
  private final DeleteMode mode;
  private final DataFilePartitions dataFilePartitions;
  private final PositionalDeleteSink sink;
  private long positions;

  public ArrowDeleteWriter(Table table, DataFilePartitions dataFilePartitions, DeleteMode mode) {
    this.table = table;
    this.mode = mode;
    this.dataFilePartitions = dataFilePartitions;
    this.sink = switch (mode) {
      // Deletion vectors are Puffin, not the table's data format, and a vector REPLACES a
      // data file's previous one, so it is seeded with what was already deleted. Never pass
      // a null loader here, or this commit resurrects those rows.
      case DELETION_VECTOR -> new PositionalDeleteSink.DeletionVectors(
          IcebergUtil.getTableOutputFileFactory(table, FileFormat.PUFFIN), new PreviousDeleteLoader(table));
      case POSITION -> {
        FileFormat format = IcebergUtil.getTableFileFormat(table);
        GenericAppenderFactory appender = IcebergUtil.getTableAppender(table);
        int currentSpecId = table.spec().specId();
        yield new PositionalDeleteSink.PositionalFiles(format,
            spec -> spec.specId() == currentSpecId ? appender : IcebergUtil.getTableAppender(table, spec),
            IcebergUtil.getTableOutputFileFactory(table, format), IcebergTableWriterFactory.DELETE_GRANULARITY);
      }
      default -> throw new IllegalArgumentException(
          "delete mode " + mode.wireName() + " does not address rows by position");
    };
  }

  /**
   * Folds one batch of positions into the deletes being built, each stamped with the spec and
   * partition of the data file it references. Those differ from the ones being written when an
   * update moves a row between partitions, or the row sits in a file written before the
   * partition spec was evolved; stamped otherwise, the old row reappears under a partition
   * filter and Iceberg 1.12+ readers reject the table.
   */
  public void add(ArrowPayload.DeletionVectorBatch batch, IcebergTableOperator operator) throws IOException {
    PartitionSpec spec = table.spec();
    for (ArrowPayload.DeletionVectorBatch.Entry entry : batch.getEntriesList()) {
      String dataFilePath = entry.getDataFilePath();
      DataFilePartitions.Located located = dataFilePartitions.locate(dataFilePath);
      if (located == null) {
        // Iceberg rejects a non-null partition on an unpartitioned spec, and vice versa.
        located = new DataFilePartitions.Located(spec, spec.isUnpartitioned()
            ? null
            : operator.partitionDataFromTypedValues(spec, entry.getPartitionValuesList()));
      }

      for (long position : entry.getPositionsList()) {
        sink.delete(dataFilePath, position, located.spec(), located.partition());
        positions++;
      }
    }
  }

  /**
   * Writes the buffered deletes and reports them.
   *
   * @return the delete files, plus (deletion vectors only) the vectors they supersede - those
   *         MUST be removed in the same commit that adds the new ones, since a data file may
   *         carry only one vector.
   */
  public DeleteWriteResult complete() throws IOException {
    sink.close();
    DeleteWriteResult result = sink.result();
    LOGGER.info("encoded {} positions of {} as {} {} delete file(s), superseding {} existing delete file(s)",
        positions, table.name(), result.deleteFiles().size(), mode.wireName(), result.rewrittenDeleteFiles().size());
    return result;
  }
}
