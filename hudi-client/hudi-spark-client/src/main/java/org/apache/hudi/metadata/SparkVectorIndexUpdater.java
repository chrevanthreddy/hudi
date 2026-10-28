/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieVectorIndexClusterStats;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.index.vector.QuantizedVector;
import org.apache.hudi.common.index.vector.RaBitQEncoder;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.spark.index.vector.TwoLevelKMeansBootstrap$;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

import scala.Tuple2;

/** Applies the secondary-index-style classifier to touched vector file groups. */
public final class SparkVectorIndexUpdater {

  private static final String CHANGES_STORAGE_LEVEL = "MEMORY_AND_DISK_SER";

  private SparkVectorIndexUpdater() {
  }

  /**
   * Builds posting and cluster-stats records for the touched file groups.
   *
   * <p>Rows are read once per file group, but routing and RaBitQ encoding are spread over
   * {@code encodeParallelism} partitions: a single rewritten file group can hold millions of rows,
   * and encoding them inside the file-group read task serialized the whole commit on one core.
   * Encoded changes are persisted under {@code cacheKey} because both postings and stats consume
   * them; the cache is released with the commit through the regular Hudi release path.
   *
   * <p>Cluster stats are absolute values: the metadata payload keeps the newest stats record, so
   * each delta is applied to the persisted stats loaded by {@code statsLoader}.
   */
  public static HoodieData<HoodieRecord> update(
      JavaRDD<FileGroupRows> fileGroups,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType,
      int encodeParallelism,
      HoodieEngineContext engineContext,
      HoodieData.HoodieDataCacheKey cacheKey,
      Function<Collection<Integer>, Map<Integer, HoodieVectorIndexClusterStats>> statsLoader,
      int generation,
      String instantTime,
      String indexPartition) {
    HoodieJavaRDD<Change> changes = HoodieJavaRDD.of(fileGroups
        .flatMap(fileGroup -> pendingChanges(fileGroup).iterator())
        .repartition(Math.max(1, encodeParallelism))
        .flatMap(pending -> encodeChange(pending, artifacts, vectorType).iterator()));
    changes.persist(CHANGES_STORAGE_LEVEL, engineContext, cacheKey);
    Map<Integer, ClusterDelta> deltas = new HashMap<>(HoodieJavaRDD.getJavaRDD(changes)
        .flatMapToPair(change -> change.statDeltas().iterator())
        .reduceByKey(ClusterDelta::plus)
        .collectAsMap());
    List<HoodieRecord> stats = clusterStatsRecords(
        deltas, statsLoader.apply(deltas.keySet()), artifacts.shardCount, generation, indexPartition);
    JavaRDD<HoodieRecord> postings = HoodieJavaRDD.getJavaRDD(changes).flatMap(change ->
        change.postingRecords(generation, instantTime, indexPartition).iterator());
    return HoodieJavaRDD.of(postings.union(
        JavaSparkContext.fromSparkContext(fileGroups.context()).parallelize(stats, 1)));
  }

  @VisibleForTesting
  static List<HoodieRecord> classifyPostingRecords(
      FileGroupRows fileGroup,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType,
      int generation,
      String instantTime,
      String indexPartition) {
    List<HoodieRecord> records = new ArrayList<>();
    for (Change change : classify(fileGroup, artifacts, vectorType)) {
      records.addAll(change.postingRecords(generation, instantTime, indexPartition));
    }
    return records;
  }

  /** Driver-local equivalent of the stats half of {@link #update}. */
  @VisibleForTesting
  static List<HoodieRecord> classifyClusterStatsRecords(
      FileGroupRows fileGroup,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType,
      Map<Integer, HoodieVectorIndexClusterStats> persistedStats,
      int generation,
      String indexPartition) {
    Map<Integer, ClusterDelta> deltas = new HashMap<>();
    for (Change change : classify(fileGroup, artifacts, vectorType)) {
      change.statDeltas().forEach(delta -> deltas.merge(delta._1, delta._2, ClusterDelta::plus));
    }
    return clusterStatsRecords(deltas, persistedStats, artifacts.shardCount, generation, indexPartition);
  }

  private static List<Change> classify(
      FileGroupRows fileGroup,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType) {
    List<Change> changes = new ArrayList<>();
    for (PendingChange pending : pendingChanges(fileGroup)) {
      changes.addAll(encodeChange(pending, artifacts, vectorType));
    }
    return changes;
  }

  /** Cheap per-file-group pass: pairs rows by key and drops unchanged vectors before any encoding. */
  private static List<PendingChange> pendingChanges(FileGroupRows fileGroup) {
    Set<String> recordKeys = new TreeSet<>(fileGroup.previousRows.keySet());
    recordKeys.addAll(fileGroup.currentRows.keySet());
    List<PendingChange> pending = new ArrayList<>();
    for (String recordKey : recordKeys) {
      SparkVectorIndexBootstrap.VectorRow previous = fileGroup.previousRows.get(recordKey);
      SparkVectorIndexBootstrap.VectorRow current = fileGroup.currentRows.get(recordKey);
      // Equal vector bytes are a no-op, mirroring RecordIndexMapper and secondary-index updates:
      // rows are grouped per file group, so only the base instant / row position can differ here.
      // The existing posting stays authoritative; exact fetch falls back to key lookup when its
      // base instant no longer matches the live base file.
      if (previous == null || current == null || !Arrays.equals(previous.vectorBytes, current.vectorBytes)) {
        pending.add(new PendingChange(previous, current, fileGroup.relocatesExistingRows));
      }
    }
    return pending;
  }

  private static List<Change> encodeChange(
      PendingChange pending,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType) {
    if (pending.previous == null) {
      EncodedRow encoded = tryEncode(pending.current, artifacts, vectorType);
      return encoded == null ? Collections.emptyList() : Collections.singletonList(new Change(
          null, encoded, pending.relocatesExistingRow ? ChangeType.RELOCATE : ChangeType.INSERT));
    }
    if (pending.current == null) {
      EncodedRow routed = tryRoute(pending.previous, artifacts, vectorType);
      return routed == null ? Collections.emptyList()
          : Collections.singletonList(new Change(routed, null, ChangeType.DELETE));
    }
    EncodedRow routed = tryRoute(pending.previous, artifacts, vectorType);
    EncodedRow encoded = tryEncode(pending.current, artifacts, vectorType);
    return routed == null && encoded == null ? Collections.emptyList()
        : Collections.singletonList(new Change(routed, encoded, ChangeType.VECTOR_UPDATE));
  }

  private static List<HoodieRecord> clusterStatsRecords(
      Map<Integer, ClusterDelta> deltas,
      Map<Integer, HoodieVectorIndexClusterStats> persistedStats,
      int defaultShardCount,
      int generation,
      String indexPartition) {
    long lastUpdatedTs = System.currentTimeMillis();
    List<HoodieRecord> records = new ArrayList<>(deltas.size());
    new TreeMap<>(deltas).forEach((clusterId, delta) -> {
      HoodieVectorIndexClusterStats persisted = persistedStats.get(clusterId);
      // Routing fields (shard count, routing version, file groups) belong to the generation and
      // must survive incremental maintenance; only the counters move.
      HoodieVectorIndexClusterStats merged = persisted == null
          ? new HoodieVectorIndexClusterStats(0, defaultShardCount, Collections.emptyList(),
              delta.liveCount, delta.deltaCount, delta.tombstoneCount, null, lastUpdatedTs)
          : new HoodieVectorIndexClusterStats(persisted.getRoutingVersion(), persisted.getShardCount(),
              persisted.getFileGroupIds(),
              persisted.getLiveCount() + delta.liveCount,
              persisted.getDeltaCount() + delta.deltaCount,
              persisted.getTombstoneCount() + delta.tombstoneCount,
              persisted.getLastRebalanceInstant(), lastUpdatedTs);
      records.add(HoodieMetadataPayload.createVectorIndexClusterStatsRecord(
          generation, clusterId, merged, indexPartition));
    });
    return records;
  }

  private static EncodedRow tryEncode(
      SparkVectorIndexBootstrap.VectorRow row,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType) {
    try {
      return encode(row, artifacts, vectorType);
    } catch (IllegalArgumentException exception) {
      return null;
    }
  }

  private static EncodedRow tryRoute(
      SparkVectorIndexBootstrap.VectorRow row,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType) {
    try {
      return route(row, artifacts, vectorType);
    } catch (IllegalArgumentException exception) {
      return null;
    }
  }

  private static EncodedRow encode(
      SparkVectorIndexBootstrap.VectorRow row,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType) {
    EncodedRow routed = route(row, artifacts, vectorType);
    float[] vector = decodeVector(row, artifacts, vectorType);
    QuantizedVector quantized = new RaBitQEncoder(
        artifacts.dimension, artifacts.bits, artifacts.seed, artifacts.assumeNormalized)
        .encodeResidual(vector, artifacts.residualEncoding ? artifacts.centroids[routed.clusterId] : null);
    return new EncodedRow(row, routed.clusterId, routed.shardId, quantized);
  }

  private static EncodedRow route(
      SparkVectorIndexBootstrap.VectorRow row,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType) {
    float[] vector = decodeVector(row, artifacts, vectorType);
    int clusterId = TwoLevelKMeansBootstrap$.MODULE$.assignOneForJava(
        artifacts.routingModel, vector, artifacts.routingExpandRatio);
    int shardId = SparkVectorIndexPostingWriter.computeShardId(
        row.recordKey, artifacts.shardCount);
    return new EncodedRow(row, clusterId, shardId, null);
  }

  private static float[] decodeVector(
      SparkVectorIndexBootstrap.VectorRow row,
      Artifacts artifacts,
      HoodieSchema.Vector.VectorElementType vectorType) {
    if (row.vectorBytes == null || row.vectorBytes.length != (long) artifacts.dimension * vectorType.getElementSize()) {
      throw new IllegalArgumentException("Vector bytes do not match the indexed dimension");
    }
    float[] vector = SparkVectorIndexBootstrap.toFloatArrayFromBytes(
        row.vectorBytes, artifacts.dimension, vectorType);
    for (float value : vector) {
      if (!Float.isFinite(value)) {
        throw new IllegalArgumentException("Vector contains a non-finite value");
      }
    }
    return vector;
  }

  /** Previous and current merged rows for one touched file group. */
  public static final class FileGroupRows implements Serializable {
    private static final long serialVersionUID = 2L;
    private final Map<String, SparkVectorIndexBootstrap.VectorRow> previousRows;
    private final Map<String, SparkVectorIndexBootstrap.VectorRow> currentRows;
    private final boolean relocatesExistingRows;

    public FileGroupRows(
        Map<String, SparkVectorIndexBootstrap.VectorRow> previousRows,
        Map<String, SparkVectorIndexBootstrap.VectorRow> currentRows) {
      this(previousRows, currentRows, false);
    }

    /**
     * @param relocatesExistingRows true when the commit only moves already-indexed rows into this
     *                              file group (clustering). Their rows were counted when first
     *                              indexed, so new rows here refresh postings without changing
     *                              live counts.
     */
    public FileGroupRows(
        Map<String, SparkVectorIndexBootstrap.VectorRow> previousRows,
        Map<String, SparkVectorIndexBootstrap.VectorRow> currentRows,
        boolean relocatesExistingRows) {
      this.previousRows = previousRows;
      this.currentRows = currentRows;
      this.relocatesExistingRows = relocatesExistingRows;
    }
  }

  /** A keyed row pair whose vector changed; routing and encoding are deferred to executors. */
  private static final class PendingChange implements Serializable {
    private static final long serialVersionUID = 1L;
    private final SparkVectorIndexBootstrap.VectorRow previous;
    private final SparkVectorIndexBootstrap.VectorRow current;
    private final boolean relocatesExistingRow;

    private PendingChange(
        SparkVectorIndexBootstrap.VectorRow previous,
        SparkVectorIndexBootstrap.VectorRow current,
        boolean relocatesExistingRow) {
      this.previous = previous;
      this.current = current;
      this.relocatesExistingRow = relocatesExistingRow;
    }
  }

  /** Immutable ACTIVE-generation artifacts required by update executors. */
  public static final class Artifacts implements Serializable {
    private static final long serialVersionUID = 1L;
    private final float[][] centroids;
    private final Object routingModel;
    private final float routingExpandRatio;
    private final int shardCount;
    private final int dimension;
    private final int bits;
    private final long seed;
    private final boolean assumeNormalized;
    private final boolean residualEncoding;

    public Artifacts(
        float[][] centroids,
        Object routingModel,
        float routingExpandRatio,
        int shardCount,
        int dimension,
        int bits,
        long seed,
        boolean assumeNormalized,
        boolean residualEncoding) {
      this.centroids = centroids;
      this.routingModel = routingModel;
      this.routingExpandRatio = routingExpandRatio;
      this.shardCount = shardCount;
      this.dimension = dimension;
      this.bits = bits;
      this.seed = seed;
      this.assumeNormalized = assumeNormalized;
      this.residualEncoding = residualEncoding;
    }
  }

  private enum ChangeType {
    INSERT,
    DELETE,
    VECTOR_UPDATE,
    /** An already-indexed row rewritten into a new file group by clustering. */
    RELOCATE
  }

  private static final class Change implements Serializable {
    private static final long serialVersionUID = 1L;
    private final EncodedRow previous;
    private final EncodedRow current;
    private final ChangeType type;

    private Change(EncodedRow previous, EncodedRow current, ChangeType type) {
      this.previous = previous;
      this.current = current;
      this.type = type;
    }

    private List<HoodieRecord> postingRecords(
        int generation, String instantTime, String indexPartition) {
      List<HoodieRecord> records = new ArrayList<>(2);
      if (previous != null
          && (current == null
          || previous.clusterId != current.clusterId
          || previous.shardId != current.shardId)) {
        records.add(HoodieMetadataPayload.createVectorIndexPostingDeleteRecord(
            generation,
            previous.row.recordKey,
            previous.clusterId,
            previous.shardId,
            instantTime,
            indexPartition));
      }
      if (current != null) {
        QuantizedVector vector = current.quantized;
        records.add(HoodieMetadataPayload.createVectorIndexPostingRecord(
            generation,
            current.row.recordKey,
            current.clusterId,
            current.shardId,
            current.row.fileId,
            current.row.partitionPath,
            current.row.baseInstantTime,
            vector.getCode(),
            vector.getExtendedCode(),
            vector.getScalar(),
            vector.getAdditiveFactor(),
            vector.getRescaleFactor(),
            vector.getAdditiveFactor1(),
            vector.getRescaleFactor1(),
            vector.getError1(),
            vector.getVectorNorm(),
            current.row.rowPosition,
            0L,
            indexPartition));
      }
      return records;
    }

    private List<Tuple2<Integer, ClusterDelta>> statDeltas() {
      if (type == ChangeType.RELOCATE) {
        // Same vector, same routing: the refreshed posting replaces the old one in place.
        return Collections.singletonList(
            new Tuple2<>(current.clusterId, new ClusterDelta(0, 1, 0)));
      }
      if (type == ChangeType.INSERT) {
        return Collections.singletonList(
            new Tuple2<>(current.clusterId, new ClusterDelta(1, 1, 0)));
      }
      if (type == ChangeType.DELETE) {
        return Collections.singletonList(
            new Tuple2<>(previous.clusterId, new ClusterDelta(-1, 0, 1)));
      }
      if (previous == null) {
        return Collections.singletonList(
            new Tuple2<>(current.clusterId, new ClusterDelta(1, 1, 0)));
      }
      if (current == null) {
        return Collections.singletonList(
            new Tuple2<>(previous.clusterId, new ClusterDelta(-1, 0, 1)));
      }
      if (previous.clusterId == current.clusterId) {
        return Collections.singletonList(
            new Tuple2<>(current.clusterId, new ClusterDelta(0, 1, 0)));
      }
      return Arrays.asList(
          new Tuple2<>(previous.clusterId, new ClusterDelta(-1, 0, 1)),
          new Tuple2<>(current.clusterId, new ClusterDelta(1, 1, 0)));
    }
  }

  private static final class EncodedRow implements Serializable {
    private static final long serialVersionUID = 1L;
    private final SparkVectorIndexBootstrap.VectorRow row;
    private final int clusterId;
    private final int shardId;
    private final QuantizedVector quantized;

    private EncodedRow(
        SparkVectorIndexBootstrap.VectorRow row,
        int clusterId,
        int shardId,
        QuantizedVector quantized) {
      this.row = row;
      this.clusterId = clusterId;
      this.shardId = shardId;
      this.quantized = quantized;
    }
  }

  private static final class ClusterDelta implements Serializable {
    private static final long serialVersionUID = 1L;
    private final long liveCount;
    private final long deltaCount;
    private final long tombstoneCount;

    private ClusterDelta(long liveCount, long deltaCount, long tombstoneCount) {
      this.liveCount = liveCount;
      this.deltaCount = deltaCount;
      this.tombstoneCount = tombstoneCount;
    }

    private ClusterDelta plus(ClusterDelta other) {
      return new ClusterDelta(
          liveCount + other.liveCount,
          deltaCount + other.deltaCount,
          tombstoneCount + other.tombstoneCount);
    }
  }
}
