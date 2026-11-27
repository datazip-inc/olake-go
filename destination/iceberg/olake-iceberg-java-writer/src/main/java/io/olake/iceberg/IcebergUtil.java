/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.olake.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;


/**
 * @author Ismail Simsek
 */
public class IcebergUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergUtil.class);
  protected static final ObjectMapper jsonObjectMapper = new ObjectMapper();
  protected static final DateTimeFormatter dtFormater = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

  private static final String BUCKET_PREFIX = "bucket[";
  private static final String TRUNCATE_PREFIX = "truncate[";
  // a partition spec commit only races other writer threads of the same stream, which all
  // ask for the same spec, so a retry almost always finds the spec already evolved
  private static final int PARTITION_SPEC_COMMIT_ATTEMPTS = 5;


  /** Creates the namespace if it does not already exist, ignoring transient catalog conflicts. */
  private static void ensureNamespace(Catalog icebergCatalog, TableIdentifier tableIdentifier) {
    if (((SupportsNamespaces) icebergCatalog).namespaceExists(tableIdentifier.namespace())) {
      return;
    }
    // multiple threads can try to create the namespace concurrently;
    // AlreadyExists means another thread won the race, which is fine.
    try {
      ((SupportsNamespaces) icebergCatalog).createNamespace(tableIdentifier.namespace());
      LOGGER.warn("Created namespace:'{}'", tableIdentifier.namespace());
    } catch (AlreadyExistsException e) {
      LOGGER.debug("Namespace '{}' already exists", tableIdentifier.namespace());
    } catch (software.amazon.awssdk.services.glue.model.ConcurrentModificationException e) {
      LOGGER.debug("Namespace '{}' is being created concurrently, proceeding", tableIdentifier.namespace());
    }
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier, Schema schema) {
    ensureNamespace(icebergCatalog, tableIdentifier);
    return icebergCatalog.createTable(tableIdentifier, schema);
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                         Schema schema, String writeFormat) {
    return createIcebergTable(icebergCatalog, tableIdentifier, schema, writeFormat, Collections.emptyList());
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                         Schema schema, String writeFormat, List<Map<String, String>> partitionTransforms) {
    return createIcebergTable(icebergCatalog, tableIdentifier, schema, writeFormat, partitionTransforms, 2);
  }

  /**
   * @param formatVersion table format version to create at. Deletion vectors need 3;
   *                      every other delete mode works on 2. Callers derive this from
   *                      {@code DeleteMode.minimumFormatVersion()} - see
   *                      {@code OlakeRowsIngester.loadOrCreateTable}.
   */
  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                         Schema schema, String writeFormat, List<Map<String, String>> partitionTransforms,
                                         int formatVersion) {

    LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}\nformatVersion:{}", tableIdentifier, schema,
        schema.identifierFieldNames(), formatVersion);

    ensureNamespace(icebergCatalog, tableIdentifier);

    // If we have partition transforms, create a PartitionSpec
    if (partitionTransforms.isEmpty()) {
      // No partitioning - create a table as before
      return icebergCatalog.buildTable(tableIdentifier, schema)
              .withProperty(FORMAT_VERSION, String.valueOf(formatVersion))
              .withProperty(DEFAULT_FILE_FORMAT, writeFormat.toLowerCase(Locale.ENGLISH))
              .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
              .create();
    } else {
      // Create a table with partitioning
      LOGGER.info("Creating table with partitioning: {}", partitionTransforms);
      
      // Start building the table
      PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
      
      // Apply each partition transform in order
      for (Map<String, String> partitionDef : partitionTransforms) {
        String field = partitionDef.get("field");
        String transform = normalizeTransform(partitionDef.get("transform"));

        // Apply the appropriate transform based on the specified type
        switch (transform) {
          case "identity":
            specBuilder.identity(field);
            break;
          case "year":
            specBuilder.year(field);
            break;
          case "month":
            specBuilder.month(field);
            break;
          case "day":
            specBuilder.day(field);
            break;
          case "hour":
            specBuilder.hour(field);
            break;
          default:
            // bucket[N] or truncate[N]; anything else is rejected by transformWidth
            if (transform.startsWith(BUCKET_PREFIX)) {
              specBuilder.bucket(field, transformWidth(field, transform, BUCKET_PREFIX));
            } else {
              specBuilder.truncate(field, transformWidth(field, transform, TRUNCATE_PREFIX));
            }
        }
      }
      
      // Create the table with the partition spec
      return icebergCatalog.buildTable(tableIdentifier, schema)
              .withProperty(FORMAT_VERSION, String.valueOf(formatVersion))
              .withProperty(DEFAULT_FILE_FORMAT, writeFormat.toLowerCase(Locale.ENGLISH))
              .withPartitionSpec(specBuilder.build())
              .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
              .create();
    }
  }

  /** Lower-cases a partition_regex transform and strips spaces, e.g. "Bucket[ 16 ]" -> "bucket[16]". */
  private static String normalizeTransform(String transform) {
    return transform == null ? "" : transform.replaceAll("\\s", "").toLowerCase(Locale.ENGLISH);
  }

  /**
   * Parses the N of a normalized bucket[N] or truncate[N] transform. An unknown or malformed
   * transform is an error rather than an identity partition: the table's spec follows the stream
   * config, so a silent substitute would be evolved into and out of the table on every edit.
   */
  private static int transformWidth(String field, String transform, String prefix) {
    if (transform.startsWith(prefix) && transform.endsWith("]")) {
      try {
        int width = Integer.parseInt(transform.substring(prefix.length(), transform.length() - 1));
        if (width > 0) {
          return width;
        }
      } catch (NumberFormatException e) {
        // reported below
      }
    }
    throw new IllegalArgumentException(String.format(
        "invalid partition transform '%s' for column '%s'; expected identity, year, month, day, hour, bucket[N] or truncate[N]",
        transform, field));
  }

  /** The partition term for a partition_regex transform, as {@link UpdatePartitionSpec} takes it. */
  public static Term toTerm(String field, String transform) {
    String normalized = normalizeTransform(transform);
    switch (normalized) {
      case "identity":
        return Expressions.ref(field);
      case "year":
        return Expressions.year(field);
      case "month":
        return Expressions.month(field);
      case "day":
        return Expressions.day(field);
      case "hour":
        return Expressions.hour(field);
      default:
        if (normalized.startsWith(BUCKET_PREFIX)) {
          return Expressions.bucket(field, transformWidth(field, normalized, BUCKET_PREFIX));
        }
        return Expressions.truncate(field, transformWidth(field, normalized, TRUNCATE_PREFIX));
    }
  }

  /**
   * The fields of a partition spec in spec order, as {field: source column, transform}, the shape
   * partition_regex arrives in. Void fields are skipped: they are what a format v1 table keeps of a
   * removed partition field, and partition nothing.
   */
  public static List<Map<String, String>> partitionFields(PartitionSpec spec, Schema schema) {
    List<Map<String, String>> fields = new ArrayList<>(spec.fields().size());
    for (PartitionField field : spec.fields()) {
      String transform = field.transform().toString();
      if ("void".equals(transform)) {
        continue;
      }
      Map<String, String> entry = new HashMap<>(2);
      entry.put("field", schema.findColumnName(field.sourceId()));
      entry.put("transform", transform);
      fields.add(entry);
    }
    return fields;
  }

  /** Identity of one partition field, independent of spec position: "column|transform". */
  private static Set<String> partitionKeys(List<Map<String, String>> fields) {
    Set<String> keys = new LinkedHashSet<>(fields.size());
    for (Map<String, String> field : fields) {
      keys.add(partitionKey(field));
    }
    return keys;
  }

  private static String partitionKey(Map<String, String> field) {
    return field.get("field") + "|" + normalizeTransform(field.get("transform"));
  }

  /**
   * Whether the spec partitions by exactly the desired fields. Order is ignored: Iceberg appends
   * fields an evolution adds, so an evolved spec can hold the desired fields in another order, and
   * the writer sends partition values in the order the spec reports back.
   */
  public static boolean specMatches(PartitionSpec spec, Schema schema, List<Map<String, String>> desired) {
    return partitionKeys(partitionFields(spec, schema)).equals(partitionKeys(desired));
  }

  /** Whether the table's current snapshot carries delete files of any kind, deletion vectors included. */
  public static boolean hasDeleteFiles(Table table) {
    Snapshot current = table.currentSnapshot();
    if (current == null || current.summary() == null) {
      return false;
    }
    String total = current.summary().get(SnapshotSummary.TOTAL_DELETE_FILES_PROP);
    return total != null && Long.parseLong(total) > 0;
  }

  /**
   * Evolves the table's default partition spec to the desired fields in place, a metadata-only
   * commit: files already written keep the spec they were written with, and only new files use the
   * new one. Changing a field's transform is a remove plus an add. A desired column the table lacks
   * is first added from the requested schema, since a spec can only reference table columns.
   *
   * <p>Only for tables that hold no delete files. A delete file applies to data files of its own
   * spec only, so one written under the new spec would miss rows still in old-spec files.
   *
   * <p>Several writer threads of a stream call this at once with the same desired fields. The first
   * commit wins; the others fail with {@link CommitFailedException}, refresh, and find the spec
   * already matching.
   *
   * @return whether this call committed a new spec
   */
  public static boolean evolvePartitionSpec(Table table, Schema requestedSchema, List<Map<String, String>> desired) {
    for (int attempt = 1; ; attempt++) {
      table.refresh();
      PartitionSpec current = table.spec();
      if (specMatches(current, table.schema(), desired)) {
        return false;
      }
      if (hasDeleteFiles(table)) {
        throw new UnsupportedOperationException(String.format(
            "cannot change the partitioning of %s in place: it has delete files, and partition evolution needs an append-only table",
            table.name()));
      }

      try {
        addMissingPartitionColumns(table, requestedSchema, desired);

        List<Map<String, String>> currentFields = partitionFields(current, table.schema());
        Set<String> currentKeys = partitionKeys(currentFields);
        Set<String> desiredKeys = partitionKeys(desired);

        UpdatePartitionSpec update = table.updateSpec();
        // removals first: re-adding a source column under a new transform checks them
        for (Map<String, String> field : currentFields) {
          if (!desiredKeys.contains(partitionKey(field))) {
            update.removeField(toTerm(field.get("field"), field.get("transform")));
          }
        }
        for (Map<String, String> field : desired) {
          if (!currentKeys.contains(partitionKey(field))) {
            update.addField(toTerm(field.get("field"), field.get("transform")));
          }
        }
        update.commit();
        table.refresh();

        LOGGER.warn("Evolved partition spec of {} from {} (id {}) to {} (id {}); existing files keep their spec",
            table.name(), currentFields, current.specId(), partitionFields(table.spec(), table.schema()),
            table.spec().specId());
        return true;
      } catch (CommitFailedException e) {
        if (attempt >= PARTITION_SPEC_COMMIT_ATTEMPTS) {
          throw e;
        }
        LOGGER.info("Partition spec commit on {} conflicted (attempt {}), refreshing: {}", table.name(), attempt,
            e.getMessage());
      }
    }
  }

  /** Adds desired partition source columns the table does not have yet, typed as the requested schema has them. */
  private static void addMissingPartitionColumns(Table table, Schema requestedSchema, List<Map<String, String>> desired) {
    UpdateSchema update = null;
    Set<String> added = new LinkedHashSet<>();
    for (Map<String, String> field : desired) {
      String column = field.get("field");
      if (table.schema().findField(column) != null || added.contains(column)) {
        continue;
      }
      Types.NestedField requested = requestedSchema.findField(column);
      if (requested == null) {
        throw new IllegalArgumentException(String.format(
            "partition column '%s' is in neither table %s nor the stream schema", column, table.name()));
      }
      if (update == null) {
        update = table.updateSchema();
      }
      update.addColumn(column, requested.type(), requested.doc());
      added.add(column);
    }
    if (update != null) {
      update.commit();
      table.refresh();
      LOGGER.warn("Added partition source columns {} to {}", added, table.name());
    }
  }

  private static SortOrder getIdentifierFieldsAsSortOrder(Schema schema) {
    SortOrder.Builder sob = SortOrder.builderFor(schema);
    for (String fieldName : schema.identifierFieldNames()) {
      sob = sob.asc(fieldName);
    }

    return sob.build();
  }

  public static Optional<Table> loadIcebergTable(Catalog icebergCatalog, TableIdentifier tableId) {
    try {
      Table table = icebergCatalog.loadTable(tableId);
      return Optional.of(table);
    } catch (NoSuchTableException e) {
      LOGGER.debug("Table not found: {}", tableId.toString());
      return Optional.empty();
    }
  }

  public static FileFormat getTableFileFormat(Table icebergTable) {
    String formatAsString = icebergTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
  }

  public static GenericAppenderFactory getTableAppender(Table icebergTable) {
    final Set<Integer> identifierFieldIds = icebergTable.schema().identifierFieldIds();
    if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
      return new GenericAppenderFactory(
          icebergTable.schema(),
          icebergTable.spec(),
          null,
          null,
          null)
          .setAll(icebergTable.properties())
          .set("write.metadata.metrics.column.file_path", "full");
    } else {
      return new GenericAppenderFactory(
          icebergTable.schema(),
          icebergTable.spec(),
          Ints.toArray(identifierFieldIds),
          TypeUtil.select(icebergTable.schema(), Sets.newHashSet(identifierFieldIds)),
          null)
          .setAll(icebergTable.properties())
          .set("write.metadata.metrics.column.file_path", "full");
    }
  }

  public static OutputFileFactory getTableOutputFileFactory(Table icebergTable, FileFormat format) {
    return OutputFileFactory.builderFor(icebergTable,
            IcebergUtil.partitionId(), 1L)
        .defaultSpec(icebergTable.spec())
        .operationId(UUID.randomUUID().toString())
        .format(format)
        .build();
  }

  public static int partitionId() {
    return Integer.parseInt(dtFormater.format(Instant.now()));
  }

  public static boolean dropIcebergTable(String namespace, String tableName, Catalog icebergCatalog) {
    try{
      TableIdentifier tableID = TableIdentifier.of(namespace, tableName);
      // Check if table exists
      if (!icebergCatalog.tableExists(tableID)) {
        LOGGER.warn("Table not found: {}", tableID.toString());
        return false;
      }
      return icebergCatalog.dropTable(tableID, false);
    } catch(Exception e){
      LOGGER.error("Failed to drop table {}.{}: {}", namespace, tableName, e.getMessage());
      throw new RuntimeException("Failed to drop table: " + namespace + "." + tableName, e);
    }
  }

}
