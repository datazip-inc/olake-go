package io.olake.iceberg.tableoperator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Which partition spec and partition a data file lives in, so the positional delete or
 * deletion vector that supersedes one of its rows can be stamped with them.
 *
 * <p>Iceberg scopes a delete to a partition: a reader applies it only to data files of the
 * same spec and partition tuple, and a partition-filtered scan prunes delete manifests by
 * it. A delete therefore has to carry the spec and partition of the data file it
 * REFERENCES. That is not the partition being written whenever an update moves a row
 * between partitions, nor the spec being written for any row still in a file written
 * before the table's partition spec was evolved. Stamped with the row being written
 * instead, the old copy stays visible - on every read for a delete file addressing several
 * data files, under a partition filter for a file-scoped one or a deletion vector - and
 * Iceberg 1.12+ readers refuse the table with "Mismatched partition tuples|specs ...
 * metadata is corrupted".
 *
 * <p>The partition comes from manifest metadata, never from the file path. A path need not
 * encode it at all ({@code write.object-storage.partitioned-paths=false}, a custom
 * {@code write.data.path}, files written by another engine), while the manifests are always
 * readable through the table's own FileIO, including under vended credentials.
 *
 * <p>One instance per writer session, shared by every writer the session creates: the
 * table's data manifests are read on the first lookup and, since manifests are immutable,
 * only new ones after that. Not thread-safe, like the rest of a session.
 */
public final class DataFilePartitions {

  private final Table table;
  private final Map<String, Located> known = Maps.newHashMap();
  private final Set<String> readManifests = Sets.newHashSet();
  // Files the caller uploaded this session, whose partition it sends itself until they
  // are committed (arrow path). Looking them up in the manifests would only cost a refresh.
  private final Set<String> uncommitted = Sets.newHashSet();
  // Paths a refreshed table did not hold. Until this session commits, such a path is
  // either one the caller is still writing or one no commit will ever add: the row index
  // only hands out committed locations and the current sync's own.
  private final Set<String> absent = Sets.newHashSet();

  public DataFilePartitions(Table table) {
    this.table = table;
  }

  /**
   * Records a data file this session is writing, which no manifest lists until it is
   * committed. Only the first call for a path does any work, so writers may call this for
   * every row.
   */
  public void recordWritten(String path, PartitionSpec spec, StructLike partition) {
    if (!known.containsKey(path)) {
      known.put(path, new Located(spec, copy(partition)));
    }
  }

  /** Records a data file the caller uploaded this session; see {@link #uncommitted}. */
  public void recordUncommitted(String path) {
    uncommitted.add(path);
  }

  /** The session's files are committed: from now on the manifests list them. */
  public void committed() {
    uncommitted.clear();
    absent.clear();
  }

  /**
   * The spec and partition of the data file at {@code path}, or null when no manifest lists
   * it: either a file the caller wrote this sync and knows the partition of, or one the
   * table does not hold, which a delete matches nothing in whatever partition it carries.
   */
  public Located locate(String path) {
    if (table.specs().size() == 1 && table.spec().isUnpartitioned()) {
      // every file the table ever held is unpartitioned: nothing to look up
      return new Located(table.spec(), null);
    }

    Located located = known.get(path);
    if (located == null && !uncommitted.contains(path) && !absent.contains(path)) {
      readNewManifests();
      located = known.get(path);
      if (located == null) {
        absent.add(path);
      }
    }
    return located;
  }

  private void readNewManifests() {
    // the path may have been committed by another writer thread of the same stream since
    // this session last looked, so read the table's current state
    table.refresh();
    Snapshot current = table.currentSnapshot();
    if (current == null) {
      return;
    }

    for (ManifestFile manifest : current.dataManifests(table.io())) {
      if (!readManifests.add(manifest.path())) {
        continue;
      }
      Map<Integer, PartitionSpec> specs = table.specs();
      try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, table.io(), specs)) {
        for (DataFile file : reader) {
          // the reader reuses its containers, so the partition is copied before it is kept
          PartitionSpec spec = specs.get(file.specId());
          known.putIfAbsent(file.location(), new Located(spec, spec.isUnpartitioned() ? null : copy(file.partition())));
        }
      } catch (IOException e) {
        throw new UncheckedIOException("failed to read manifest " + manifest.path() + " of " + table.name(), e);
      }
    }
  }

  private static StructLike copy(StructLike partition) {
    if (partition instanceof PartitionKey key) {
      return key.copy();
    }
    if (partition instanceof PartitionData data) {
      return data.copy();
    }
    return partition;
  }

  /** Where a data file lives. The partition is null under an unpartitioned spec, as Iceberg requires. */
  public record Located(PartitionSpec spec, StructLike partition) {
  }
}
