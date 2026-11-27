package io.olake.iceberg;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evolves an existing table's partition spec in place when a stream's partition_regex changes,
 * instead of the table being dropped and resynced.
 *
 * <p>Iceberg partitioning is hidden: data files written before the change keep the spec they
 * were written under, files written after use the new one, and readers plan each file against
 * its own spec. What makes this safe for upserts is that every positional delete and deletion
 * vector carries the spec and partition of the file it references (see
 * {@code DataFilePartitions}). Equality deletes cannot: a reader applies one only to data files
 * of its own spec and partition, so an equality delete written under the new spec never reaches
 * a row in a file of the old one. Streams writing equality deletes therefore keep their spec.
 *
 * <p>OLake applies the partition_regex it is configured with, not whatever spec the table
 * happens to have: the fields it last applied are recorded in {@link #APPLIED_PROPERTY}, and
 * the spec is only evolved when the configured fields differ from that record. A spec evolved
 * by another engine since is left alone until the stream's own partition_regex changes.
 */
public final class PartitionEvolution {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionEvolution.class);

  /** Table property holding the partition fields OLake last applied, as {@code column:transform;...}. */
  public static final String APPLIED_PROPERTY = "olake.partition-fields";

  private static final Pattern WIDTH_TRANSFORM = Pattern.compile("^(bucket|truncate)\\[\\s*(\\d+)\\s*]$");
  private static final int COMMIT_ATTEMPTS = 5;
  // every writer thread of a stream opens the table at once; one evolves it, the rest see the result
  private static final Map<String, Object> LOCKS = new ConcurrentHashMap<>();

  private PartitionEvolution() {
  }

  /** One partition field, compared by source column and transform. */
  public record Field(String column, String transform) {
    @Override
    public String toString() {
      return column + ":" + transform;
    }
  }

  /**
   * The fields a partition_regex asks for, in its order. A void transform asks for nothing; an
   * unknown one becomes identity, the same fallback {@link IcebergUtil#createIcebergTable} takes.
   */
  public static List<Field> requested(List<Map<String, String>> transforms) {
    List<Field> fields = new ArrayList<>();
    if (transforms == null) {
      return fields;
    }
    for (Map<String, String> definition : transforms) {
      String transform = canonicalTransform(definition.get("transform"));
      if (!transform.equals("void")) {
        fields.add(new Field(definition.get("field"), transform));
      }
    }
    return fields;
  }

  /** The record {@link #APPLIED_PROPERTY} keeps for these fields. */
  public static String applied(List<Field> fields) {
    return fields.stream().map(Field::toString).collect(Collectors.joining(";"));
  }

  /** The fields of the table's current spec, void ones (a v1 table's removed fields) left out. */
  public static List<Field> current(Table table) {
    List<Field> fields = new ArrayList<>();
    for (PartitionField field : table.spec().fields()) {
      if (field.transform().isVoid()) {
        continue;
      }
      fields.add(new Field(table.schema().findColumnName(field.sourceId()), field.transform().toString()));
    }
    return fields;
  }

  /**
   * Brings the table's spec in line with the stream's partition_regex if it changed since OLake
   * last applied one.
   *
   * @param evolutionAllowed false for a stream writing equality deletes, whose spec is kept
   * @return whether the spec was evolved
   */
  public static boolean apply(Table table, List<Map<String, String>> transforms, boolean evolutionAllowed) {
    List<Field> requested = requested(transforms);
    String record = applied(requested);

    synchronized (LOCKS.computeIfAbsent(table.name(), name -> new Object())) {
      for (int attempt = 1; ; attempt++) {
        table.refresh();
        String lastApplied = table.properties().get(APPLIED_PROPERTY);
        if (record.equals(lastApplied)) {
          // the configuration has not changed; a spec another engine evolved since is kept
          return false;
        }

        List<Field> current = current(table);
        boolean specMatches = new LinkedHashSet<>(current).equals(new LinkedHashSet<>(requested));
        if (!specMatches && !evolutionAllowed) {
          LOGGER.warn("partition_regex of {} asks for [{}], but the table is partitioned by [{}]. A stream writing "
                  + "equality deletes keeps its partition spec: an equality delete written under a new spec never "
                  + "reaches a row in a file of the old one. Clear the destination and resync to repartition, or "
                  + "use update_type pos or dv.",
              table.name(), record, applied(current));
          return false;
        }

        try {
          Transaction transaction = table.newTransaction();
          if (!specMatches) {
            evolve(transaction.updateSpec(), current, requested, table);
          }
          transaction.updateProperties().set(APPLIED_PROPERTY, record).commit();
          transaction.commitTransaction();
        } catch (CommitFailedException e) {
          if (attempt >= COMMIT_ATTEMPTS) {
            throw e;
          }
          LOGGER.info("partition spec update of {} raced another commit, retrying ({}/{})", table.name(), attempt,
              COMMIT_ATTEMPTS);
          continue;
        }

        table.refresh();
        if (specMatches) {
          LOGGER.info("recorded partition fields [{}] of {}", record, table.name());
          return false;
        }
        LOGGER.warn("evolved partition spec of {} from [{}] to [{}] (spec id {}); existing data files keep the "
                + "spec they were written under", table.name(), applied(current), record, table.spec().specId());
        return true;
      }
    }
  }

  private static void evolve(UpdatePartitionSpec update, List<Field> current, List<Field> requested, Table table) {
    Set<Field> wanted = new LinkedHashSet<>(requested);
    for (PartitionField field : table.spec().fields()) {
      if (field.transform().isVoid()) {
        continue;
      }
      Field existing = new Field(table.schema().findColumnName(field.sourceId()), field.transform().toString());
      if (!wanted.contains(existing)) {
        update.removeField(field.name());
      }
    }
    Set<Field> have = new LinkedHashSet<>(current);
    for (Field field : requested) {
      if (!have.contains(field)) {
        if (table.schema().findField(field.column()) == null) {
          throw new IllegalStateException(String.format(
              "cannot partition %s by %s: the table has no column %s", table.name(), field, field.column()));
        }
        update.addField(term(field));
      }
    }
    update.commit();
  }

  static Term term(Field field) {
    String column = field.column();
    String transform = field.transform();
    switch (transform) {
      case "identity":
        return Expressions.ref(column);
      case "year":
        return Expressions.year(column);
      case "month":
        return Expressions.month(column);
      case "day":
        return Expressions.day(column);
      case "hour":
        return Expressions.hour(column);
      default:
        Matcher width = WIDTH_TRANSFORM.matcher(transform);
        if (!width.matches()) {
          throw new IllegalArgumentException("unsupported partition transform: " + transform);
        }
        int argument = Integer.parseInt(width.group(2));
        return width.group(1).equals("bucket")
            ? Expressions.bucket(column, argument)
            : Expressions.truncate(column, argument);
    }
  }

  /** The transform as Iceberg prints it ({@code identity}, {@code day}, {@code bucket[16]}, ...). */
  static String canonicalTransform(String raw) {
    String transform = Objects.requireNonNullElse(raw, "").trim().toLowerCase(Locale.ROOT);
    switch (transform) {
      case "identity":
      case "year":
      case "month":
      case "day":
      case "hour":
      case "void":
        return transform;
      default:
        Matcher width = WIDTH_TRANSFORM.matcher(transform);
        if (width.matches()) {
          return width.group(1) + "[" + Integer.parseInt(width.group(2)) + "]";
        }
        return "identity";
    }
  }
}
