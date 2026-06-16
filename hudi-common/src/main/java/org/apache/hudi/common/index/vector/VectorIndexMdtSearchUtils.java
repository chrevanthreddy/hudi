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

package org.apache.hudi.common.index.vector;

import org.apache.hudi.avro.model.HoodieVectorIndexInfo;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.RawKey;
import org.apache.hudi.metadata.VectorClusterRawKey;
import org.apache.hudi.metadata.VectorPostingPrefixRawKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared helper for MDT-native vector posting lookup and approximate candidate reduction.
 *
 * <p>This provides the missing second-stage building block after coarse IVF cluster pruning:
 * exact HFile lookups of cluster metadata, prefix scans over {@code P|gen|cluster|shard|},
 * approximate RaBitQ scoring, and direct file-slice targeting from posting payload metadata.
 */
public final class VectorIndexMdtSearchUtils {

  private static final Logger LOG = LoggerFactory.getLogger(VectorIndexMdtSearchUtils.class);
  private static final int TOP_K_REDUCER_KEY = 0;

  private VectorIndexMdtSearchUtils() {
  }

  public static Map<Integer, Integer> readClusterShardCounts(HoodieTableMetadata metadataTable,
                                                             String indexPartition,
                                                             int generationId,
                                                             Collection<Integer> clusterIds) {
    if (clusterIds == null || clusterIds.isEmpty()) {
      return Collections.emptyMap();
    }

    List<RawKey> clusterKeys = new ArrayList<>(clusterIds.size());
    for (Integer clusterId : clusterIds) {
      clusterKeys.add(new VectorClusterRawKey(generationId, clusterId));
    }

    List<HoodieRecord<HoodieMetadataPayload>> records = metadataTable
        .getRecordsByKeyPrefixes(HoodieListData.eager(clusterKeys), indexPartition, true)
        .collectAsList();

    Map<Integer, Integer> shardCounts = new HashMap<>();
    for (HoodieRecord<HoodieMetadataPayload> record : records) {
      Option<HoodieVectorIndexInfo> infoOpt = getVectorInfo(record);
      if (!infoOpt.isPresent()) {
        continue;
      }

      HoodieVectorIndexInfo info = infoOpt.get();
      if (!HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_CLUSTER.equals(info.getEntryType())) {
        continue;
      }
      shardCounts.put(info.getClusterId(), Math.max(1, info.getShardCount()));
    }
    return shardCounts;
  }

  public static Map<Integer, Set<String>> readClusterFileGroups(HoodieTableMetadata metadataTable,
                                                                 String indexPartition,
                                                                 int generationId,
                                                                 Collection<Integer> clusterIds,
                                                                 Collection<String> partitionPaths,
                                                                 boolean shouldLoadInMemory) {
    if (clusterIds == null || clusterIds.isEmpty()) {
      return Collections.emptyMap();
    }

    Set<String> partitionFilter =
        partitionPaths == null || partitionPaths.isEmpty() ? Collections.emptySet() : new HashSet<>(partitionPaths);
    List<RawKey> clusterKeys = new ArrayList<>(clusterIds.size());
    for (Integer clusterId : clusterIds) {
      clusterKeys.add(new VectorClusterRawKey(generationId, clusterId));
    }

    List<HoodieRecord<HoodieMetadataPayload>> records = metadataTable
        .getRecordsByKeyPrefixes(HoodieListData.eager(clusterKeys), indexPartition, shouldLoadInMemory)
        .collectAsList();

    Map<Integer, Set<String>> clusterToFileGroups = new HashMap<>();
    for (HoodieRecord<HoodieMetadataPayload> record : records) {
      Option<HoodieVectorIndexInfo> infoOpt = getVectorInfo(record);
      if (!infoOpt.isPresent()) {
        continue;
      }

      HoodieVectorIndexInfo info = infoOpt.get();
      if (!HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_CLUSTER.equals(info.getEntryType())
          || info.getFileGroupIds() == null
          || info.getFileGroupIds().isEmpty()) {
        continue;
      }
      if (!partitionFilter.isEmpty()
          && info.getPartitionPath() != null
          && !partitionFilter.contains(info.getPartitionPath())) {
        continue;
      }
      clusterToFileGroups
          .computeIfAbsent(info.getClusterId(), ignored -> new HashSet<>())
          .addAll(info.getFileGroupIds());
    }
    return clusterToFileGroups;
  }

  public static List<VectorPostingPrefixRawKey> buildPostingPrefixes(int generationId,
                                                                     Map<Integer, Integer> clusterShardCounts) {
    if (clusterShardCounts == null || clusterShardCounts.isEmpty()) {
      return Collections.emptyList();
    }

    List<Map.Entry<Integer, Integer>> clusters = new ArrayList<>(clusterShardCounts.entrySet());
    clusters.sort(Map.Entry.comparingByKey());

    List<VectorPostingPrefixRawKey> prefixes = new ArrayList<>();
    for (Map.Entry<Integer, Integer> entry : clusters) {
      int clusterId = entry.getKey();
      int shardCount = Math.max(1, entry.getValue());
      for (int shardId = 0; shardId < shardCount; shardId++) {
        prefixes.add(new VectorPostingPrefixRawKey(generationId, clusterId, shardId));
      }
    }
    return prefixes;
  }

  public static HoodieData<PostingMatch> readPostingMatches(HoodieTableMetadata metadataTable,
                                                            String indexPartition,
                                                            int generationId,
                                                            Map<Integer, Integer> clusterShardCounts,
                                                            boolean shouldLoadInMemory) {
    List<VectorPostingPrefixRawKey> postingPrefixes = buildPostingPrefixes(generationId, clusterShardCounts);
    if (postingPrefixes.isEmpty()) {
      return HoodieListData.eager(Collections.emptyList());
    }

    List<RawKey> rawKeys = new ArrayList<>(postingPrefixes);
    HoodieData<PostingMatch> matches = metadataTable.getRecordsByKeyPrefixes(HoodieListData.eager(rawKeys), indexPartition, shouldLoadInMemory)
        .flatMap(record -> {
          Option<HoodieVectorIndexInfo> infoOpt = getVectorInfo(record);
          if (!infoOpt.isPresent()) {
            return Collections.<PostingMatch>emptyIterator();
          }

          HoodieVectorIndexInfo info = infoOpt.get();
          if (!HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_POSTING.equals(info.getEntryType())
              || record.getData().isDeleted()
              || info.getBinaryCode() == null) {
            return Collections.<PostingMatch>emptyIterator();
          }

          String recordKey = info.getRecordKey();
          if (recordKey == null) {
            return Collections.<PostingMatch>emptyIterator();
          }

          ByteBuffer duplicated = info.getBinaryCode().duplicate();
          byte[] binaryCode = new byte[duplicated.remaining()];
          duplicated.get(binaryCode);
          PostingMatch match = new PostingMatch(
              recordKey,
              info.getClusterId(),
              info.getShardId(),
              info.getFileGroupId(),
              info.getPartitionPath(),
              info.getBaseInstantTime(),
              binaryCode,
              info.getScalar());
          return Collections.singletonList(match).iterator();
        });
    return matches.mapPartitions(iterator -> {
      long startMs = System.currentTimeMillis();
      List<PostingMatch> partitionMatches = new ArrayList<>();
      long binaryCodeBytes = 0L;
      Set<Integer> clusters = new HashSet<>();
      Set<String> fileGroups = new HashSet<>();
      while (iterator.hasNext()) {
        PostingMatch match = iterator.next();
        partitionMatches.add(match);
        if (match.getBinaryCode() != null) {
          binaryCodeBytes += match.getBinaryCode().length;
        }
        clusters.add(match.getClusterId());
        if (match.getFileGroupId() != null) {
          fileGroups.add(match.getFileGroupId());
        }
      }
      LOG.info("[vector_search][stage][read_postings] prefixes={} shouldLoadInMemory={} matches={} codeBytes={} distinctClusters={} distinctFileGroups={} elapsedMs={}",
          postingPrefixes.size(),
          shouldLoadInMemory,
          partitionMatches.size(),
          binaryCodeBytes,
          clusters.size(),
          fileGroups.size(),
          System.currentTimeMillis() - startMs);
      return partitionMatches.iterator();
    }, true);
  }

  public static Map<Integer, Set<String>> collectClusterToFileGroups(HoodieTableMetadata metadataTable,
                                                                     String indexPartition,
                                                                     int generationId,
                                                                     Map<Integer, Integer> clusterShardCounts,
                                                                     Collection<String> partitionPaths,
                                                                     boolean shouldLoadInMemory) {
    if (clusterShardCounts == null || clusterShardCounts.isEmpty()) {
      return Collections.emptyMap();
    }

    Set<String> partitionFilter =
        partitionPaths == null || partitionPaths.isEmpty() ? Collections.emptySet() : new HashSet<>(partitionPaths);
    List<PostingMatch> postingMatches = readPostingMatches(
        metadataTable, indexPartition, generationId, clusterShardCounts, shouldLoadInMemory).collectAsList();
    Map<Integer, Set<String>> clusterToFileGroups = new HashMap<>();
    for (PostingMatch match : postingMatches) {
      if (match.getFileGroupId() == null) {
        continue;
      }
      if (!partitionFilter.isEmpty()
          && match.getPartitionPath() != null
          && !partitionFilter.contains(match.getPartitionPath())) {
        continue;
      }
      clusterToFileGroups
          .computeIfAbsent(match.getClusterId(), ignored -> new HashSet<>())
          .add(match.getFileGroupId());
    }
    return clusterToFileGroups;
  }

  /**
   * Reads all posting records from the vector index partition and returns a mapping
   * from record key to IVF cluster ID.  This is used for vector-aware clustering:
   * by sorting main table records by their cluster assignment during Hudi clustering,
   * records from the same IVF cluster are co-located in the same file groups, which
   * dramatically reduces I/O during the exact-read phase of vector search.
   */
  public static HoodiePairData<String, Integer> readClusterAssignments(
      HoodieTableMetadata metadataTable,
      String indexPartition,
      int generationId,
      Map<Integer, Integer> clusterShardCounts,
      boolean shouldLoadInMemory) {
    HoodieData<PostingMatch> postings = readPostingMatches(
        metadataTable, indexPartition, generationId, clusterShardCounts, shouldLoadInMemory);
    return postings.mapToPair(p -> Pair.of(p.getRecordKey(), p.getClusterId()));
  }

  public static HoodieData<ScoredPostingMatch> scorePostingMatches(HoodieData<PostingMatch> postingMatches,
                                                                   float[] queryVector,
                                                                   int dimension,
                                                                   long randomSeed,
                                                                   boolean assumeNormalized) {
    long queryPrepStartMs = System.currentTimeMillis();
    RaBitQEncoder encoder = new RaBitQEncoder(dimension, randomSeed, assumeNormalized);
    RaBitQEncoder.RaBitQQueryState queryState = (RaBitQEncoder.RaBitQQueryState) encoder.encodeQuery(queryVector);
    long queryPrepMs = System.currentTimeMillis() - queryPrepStartMs;
    LOG.info("[vector_search][stage][score_postings_setup] queryDim={} queryCodeBytes={} elapsedMs={}",
        dimension,
        queryState.binaryCode.length,
        queryPrepMs);
    return postingMatches.mapPartitions(iterator -> {
      long partitionStartMs = System.currentTimeMillis();
      if (!iterator.hasNext()) {
        LOG.info("[vector_search][stage][score_postings] input=0 output=0 queryDim={} computeMs=0 elapsedMs={}",
            dimension,
            System.currentTimeMillis() - partitionStartMs);
        return Collections.<ScoredPostingMatch>emptyIterator();
      }

      List<ScoredPostingMatch> scored = new ArrayList<>();
      float minDistance = Float.POSITIVE_INFINITY;
      float maxDistance = Float.NEGATIVE_INFINITY;
      long computeStartMs = System.currentTimeMillis();
      while (iterator.hasNext()) {
        PostingMatch match = iterator.next();
        float effectiveScalar = match.getScalar() != null ? match.getScalar() : 1.0f;
        float approxDistance = RaBitQEncoder.estimateDistance(
            queryState.binaryCode,
            match.getBinaryCode(),
            effectiveScalar,
            dimension);
        scored.add(new ScoredPostingMatch(match, approxDistance, null));
        minDistance = Math.min(minDistance, approxDistance);
        maxDistance = Math.max(maxDistance, approxDistance);
      }
      long computeMs = System.currentTimeMillis() - computeStartMs;
      LOG.info("[vector_search][stage][score_postings] input={} output={} queryDim={} minDist={} maxDist={} computeMs={} elapsedMs={}",
          scored.size(),
          scored.size(),
          dimension,
          scored.isEmpty() ? "n/a" : minDistance,
          scored.isEmpty() ? "n/a" : maxDistance,
          computeMs,
          System.currentTimeMillis() - partitionStartMs);
      return scored.iterator();
    }, true);
  }

  public static HoodieData<ScoredPostingMatch> attachRecordLocations(HoodieTableMetadata metadataTable,
                                                                     HoodieData<ScoredPostingMatch> scoredPostingMatches) {
    HoodiePairData<String, ScoredPostingMatch> scoredByRecordKey =
        scoredPostingMatches.mapToPair(candidate -> Pair.of(candidate.getRecordKey(), candidate));
    HoodiePairData<String, HoodieRecordGlobalLocation> locations =
        metadataTable.readRecordIndexLocationsWithKeys(scoredByRecordKey.keys().distinct());

    return scoredByRecordKey.leftOuterJoin(locations)
        .flatMapValues(joined -> {
          if (!joined.getRight().isPresent()) {
            return Collections.<ScoredPostingMatch>emptyIterator();
          }
          return Collections.singletonList(joined.getLeft().withLocation(joined.getRight().get())).iterator();
        })
        .values();
  }

  public static HoodieData<ScoredPostingMatch> selectTopK(HoodieData<ScoredPostingMatch> candidates, int topK) {
    if (topK <= 0) {
      return HoodieListData.eager(Collections.emptyList());
    }

    HoodiePairData<Integer, List<ScoredPostingMatch>> partialTopK = candidates
        .mapPartitions(iterator -> {
          long startMs = System.currentTimeMillis();
          List<ScoredPostingMatch> localTopK = new ArrayList<>();
          while (iterator.hasNext()) {
            localTopK.add(iterator.next());
          }
          int inputCount = localTopK.size();
          trimTopK(localTopK, topK);
          LOG.info("[vector_search][stage][select_topk_local] input={} kept={} topK={} elapsedMs={}",
              inputCount,
              localTopK.size(),
              topK,
              System.currentTimeMillis() - startMs);
          if (localTopK.isEmpty()) {
            return Collections.<Pair<Integer, List<ScoredPostingMatch>>>emptyIterator();
          }
          return Collections.singletonList(Pair.of(TOP_K_REDUCER_KEY, localTopK)).iterator();
        }, true)
        .mapToPair(pair -> Pair.of(pair.getLeft(), pair.getRight()));

    return partialTopK
        .reduceByKey((left, right) -> mergeTopK(left, right, topK), 1)
        .values()
        .flatMap(List::iterator)
        .mapPartitions(iterator -> {
          long startMs = System.currentTimeMillis();
          List<ScoredPostingMatch> finalTopK = new ArrayList<>();
          while (iterator.hasNext()) {
            finalTopK.add(iterator.next());
          }
          LOG.info("[vector_search][stage][select_topk_final] kept={} topK={} elapsedMs={}",
              finalTopK.size(),
              topK,
              System.currentTimeMillis() - startMs);
          return finalTopK.iterator();
        }, true);
  }

  public static List<ScoredPostingMatch> collectTopKWithLocations(HoodieTableMetadata metadataTable,
                                                                  String indexPartition,
                                                                  int generationId,
                                                                  Map<Integer, Integer> clusterShardCounts,
                                                                  float[] queryVector,
                                                                  int dimension,
                                                                  long randomSeed,
                                                                  boolean assumeNormalized,
                                                                  int topK) {
    HoodieData<ScoredPostingMatch> topKData = selectTopK(
        attachRecordLocations(
            metadataTable,
            scorePostingMatches(
                readPostingMatches(metadataTable, indexPartition, generationId, clusterShardCounts, false),
                queryVector,
                dimension,
                randomSeed,
                assumeNormalized)),
        topK);

    try {
      return topKData.collectAsList();
    } finally {
      topKData.unpersistWithDependencies();
    }
  }

  private static Option<HoodieVectorIndexInfo> getVectorInfo(HoodieRecord<HoodieMetadataPayload> record) {
    return record.getData().getVectorIndexMetadata();
  }

  static List<ScoredPostingMatch> mergeTopK(List<ScoredPostingMatch> left,
                                            List<ScoredPostingMatch> right,
                                            int topK) {
    List<ScoredPostingMatch> merged = new ArrayList<>(left.size() + right.size());
    merged.addAll(left);
    merged.addAll(right);
    trimTopK(merged, topK);
    return merged;
  }

  private static void trimTopK(List<ScoredPostingMatch> candidates, int topK) {
    candidates.sort(Comparator
        .comparingDouble(ScoredPostingMatch::getApproxDistance)
        .thenComparing(ScoredPostingMatch::getRecordKey));
    if (candidates.size() > topK) {
      candidates.subList(topK, candidates.size()).clear();
    }
  }

  public static class PostingMatch implements Serializable {
    private final String recordKey;
    private final int clusterId;
    private final int shardId;
    private final String fileGroupId;
    private final String partitionPath;
    private final String baseInstantTime;
    private final byte[] binaryCode;
    private final Float scalar;

    public PostingMatch(String recordKey,
                        int clusterId,
                        int shardId,
                        String fileGroupId,
                        String partitionPath,
                        String baseInstantTime,
                        byte[] binaryCode,
                        Float scalar) {
      this.recordKey = recordKey;
      this.clusterId = clusterId;
      this.shardId = shardId;
      this.fileGroupId = fileGroupId;
      this.partitionPath = partitionPath;
      this.baseInstantTime = baseInstantTime;
      this.binaryCode = binaryCode;
      this.scalar = scalar;
    }

    public String getRecordKey() {
      return recordKey;
    }

    public int getClusterId() {
      return clusterId;
    }

    public int getShardId() {
      return shardId;
    }

    public String getFileGroupId() {
      return fileGroupId;
    }

    public String getPartitionPath() {
      return partitionPath;
    }

    public String getBaseInstantTime() {
      return baseInstantTime;
    }

    public byte[] getBinaryCode() {
      return binaryCode;
    }

    public Float getScalar() {
      return scalar;
    }
  }

  public static final class ScoredPostingMatch extends PostingMatch {
    private final float approxDistance;
    private final HoodieRecordGlobalLocation location;

    public ScoredPostingMatch(PostingMatch match, float approxDistance, HoodieRecordGlobalLocation location) {
      super(
          match.getRecordKey(),
          match.getClusterId(),
          match.getShardId(),
          match.getFileGroupId(),
          match.getPartitionPath(),
          match.getBaseInstantTime(),
          match.getBinaryCode(),
          match.getScalar());
      this.approxDistance = approxDistance;
      this.location = location;
    }

    public float getApproxDistance() {
      return approxDistance;
    }

    public HoodieRecordGlobalLocation getLocation() {
      return location;
    }

    public ScoredPostingMatch withLocation(HoodieRecordGlobalLocation newLocation) {
      return new ScoredPostingMatch(this, approxDistance, newLocation);
    }
  }
}
