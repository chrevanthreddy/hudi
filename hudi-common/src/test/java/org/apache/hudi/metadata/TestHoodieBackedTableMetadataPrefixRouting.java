/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieBackedTableMetadataPrefixRouting {

  private static final String VECTOR_PARTITION = HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX_PREFIX + "embedding_idx";
  private static final int GENERATION = 1;

  @Test
  void postingPrefixesReadOnlyTheirClustersFileGroups() {
    List<FileSlice> slices = fileSlices(VECTOR_PARTITION, 512);
    // 32 probed clusters with 3 shards each, as one approximate query issues them.
    List<String> prefixes = new ArrayList<>();
    IntStream.range(0, 32).forEach(i -> IntStream.range(0, 3).forEach(shard ->
        prefixes.add(VectorIndexMetadataKey.postingPrefix(GENERATION, 7 + i * 517, shard))));
    prefixes.sort(StringUtils.UTF8_LEXICOGRAPHIC_COMPARATOR);

    List<Pair<FileSlice, List<String>>> routed =
        HoodieBackedTableMetadata.routeKeyPrefixesToFileSlices(VECTOR_PARTITION, prefixes, slices);

    assertEquals(32, routed.size(), "one file group per probed cluster, not all 512");
    assertEquals(prefixes.size(), routed.stream().mapToInt(p -> p.getRight().size()).sum(), "every prefix routed exactly once");
    for (Pair<FileSlice, List<String>> sliceAndPrefixes : routed) {
      for (String prefix : sliceAndPrefixes.getRight()) {
        // Must land on the slice the writer tags for the same key and file-group count.
        int writerIndex = HoodieTableMetadataUtil.mapVectorPostingKeyToFileGroupIndex(prefix, slices.size());
        assertEquals(slices.get(writerIndex), sliceAndPrefixes.getLeft());
      }
      assertSorted(sliceAndPrefixes.getRight());
    }
  }

  @Test
  void prefixesRouteToTheSameSliceAsTheRowsTheyPrefix() {
    List<FileSlice> slices = fileSlices(VECTOR_PARTITION, 64);
    int cluster = 1234;
    String prefix = VectorIndexMetadataKey.postingPrefix(GENERATION, cluster, 2);
    int blockRowIndex = HoodieTableMetadataUtil.mapVectorPostingKeyToFileGroupIndex(
        VectorIndexMetadataKey.postingBlock(GENERATION, cluster, 2, 9L), slices.size());
    int deltaRowIndex = HoodieTableMetadataUtil.mapVectorPostingKeyToFileGroupIndex(
        VectorIndexMetadataKey.postingDelta(GENERATION, cluster, 2, "key-1"), slices.size());

    List<Pair<FileSlice, List<String>>> routed = HoodieBackedTableMetadata.routeKeyPrefixesToFileSlices(
        VECTOR_PARTITION, Arrays.asList(prefix), slices);

    assertEquals(1, routed.size());
    assertEquals(slices.get(blockRowIndex), routed.get(0).getLeft());
    assertEquals(slices.get(deltaRowIndex), routed.get(0).getLeft());
  }

  @Test
  void exactArtifactKeysReadOnlyTheWritersFileGroup() {
    List<FileSlice> slices = fileSlices(VECTOR_PARTITION, 512);
    // The cold metadata-cache and updater lookups: each is one exact key of a fixed-length family.
    List<String> exactKeys = Arrays.asList(
        VectorIndexMetadataKey.activeManifest(),
        VectorIndexMetadataKey.manifest(GENERATION),
        VectorIndexMetadataKey.quantizer(GENERATION, 0),
        VectorIndexMetadataKey.centroids(GENERATION, 0),
        VectorIndexMetadataKey.centroids(GENERATION, 37),
        VectorIndexMetadataKey.clusterStats(GENERATION, 16383));
    for (String key : exactKeys) {
      List<Pair<FileSlice, List<String>>> routed = HoodieBackedTableMetadata.routeKeyPrefixesToFileSlices(
          VECTOR_PARTITION, Collections.singletonList(key), slices);
      assertEquals(1, routed.size(), "exact artifact key must read one file group, not all 512");
      // The writer tags the row with the same mapping over the same file-id-sorted slices.
      assertEquals(slices.get(HoodieTableMetadataUtil.mapVectorPostingKeyToFileGroupIndex(key, slices.size())),
          routed.get(0).getLeft());
    }
  }

  @Test
  void batchedExactKeysEachReachTheirOwnFileGroup() {
    List<FileSlice> slices = fileSlices(VECTOR_PARTITION, 512);
    List<String> keys = new ArrayList<>(Arrays.asList(
        VectorIndexMetadataKey.manifest(GENERATION),
        VectorIndexMetadataKey.quantizer(GENERATION, 0)));
    IntStream.range(0, 40).forEach(chunk -> keys.add(VectorIndexMetadataKey.centroids(GENERATION, chunk)));
    keys.sort(StringUtils.UTF8_LEXICOGRAPHIC_COMPARATOR);

    List<Pair<FileSlice, List<String>>> routed =
        HoodieBackedTableMetadata.routeKeyPrefixesToFileSlices(VECTOR_PARTITION, keys, slices);

    assertEquals(keys.size(), routed.stream().mapToInt(p -> p.getRight().size()).sum(), "every key routed exactly once");
    for (Pair<FileSlice, List<String>> sliceAndKeys : routed) {
      sliceAndKeys.getRight().forEach(key -> assertEquals(
          slices.get(HoodieTableMetadataUtil.mapVectorPostingKeyToFileGroupIndex(key, slices.size())), sliceAndKeys.getLeft()));
      assertSorted(sliceAndKeys.getRight());
    }
  }

  @Test
  void prefixesThatCanSpanFileGroupsKeepFullFanOut() {
    List<FileSlice> slices = fileSlices(VECTOR_PARTITION, 16);
    // Family-only and truncated prefixes match rows hashed to many file groups; markers are
    // variable length and are read by prefix scan, so neither may be routed.
    List<String> unroutable = Arrays.asList(
        familyPrefix(VectorIndexMetadataKey.FAMILY_MANIFEST),
        familyPrefix(VectorIndexMetadataKey.FAMILY_CENTROIDS),
        familyPrefix(VectorIndexMetadataKey.FAMILY_QUANTIZER),
        familyPrefix(VectorIndexMetadataKey.FAMILY_CLUSTER_STATS),
        familyPrefix(VectorIndexMetadataKey.FAMILY_POSTING),
        VectorIndexMetadataKey.centroids(GENERATION, 0).substring(0, 5),
        VectorIndexMetadataKey.sourceInstantMarkerPrefix(GENERATION),
        VectorIndexMetadataKey.sourceInstantMarker(GENERATION, "20261014204057668"));
    for (String prefix : unroutable) {
      List<String> single = Collections.singletonList(prefix);
      assertFullFanOut(HoodieBackedTableMetadata.routeKeyPrefixesToFileSlices(VECTOR_PARTITION, single, slices), slices, single);
    }
  }

  @Test
  void oneUnroutablePrefixKeepsFullFanOutForTheWholeLookup() {
    List<FileSlice> slices = fileSlices(VECTOR_PARTITION, 16);
    List<String> prefixes = new ArrayList<>(Arrays.asList(
        VectorIndexMetadataKey.manifest(GENERATION),
        familyPrefix(VectorIndexMetadataKey.FAMILY_CLUSTER_STATS)));
    prefixes.sort(StringUtils.UTF8_LEXICOGRAPHIC_COMPARATOR);

    assertFullFanOut(HoodieBackedTableMetadata.routeKeyPrefixesToFileSlices(VECTOR_PARTITION, prefixes, slices), slices, prefixes);
  }

  @Test
  void prefixWithoutClusterIdKeepsFullFanOut() {
    List<FileSlice> slices = fileSlices(VECTOR_PARTITION, 16);
    List<String> prefixes = new ArrayList<>(Arrays.asList(
        VectorIndexMetadataKey.postingPrefix(GENERATION, 3, 0),
        VectorIndexMetadataKey.sourceInstantMarkerPrefix(GENERATION)));
    prefixes.sort(StringUtils.UTF8_LEXICOGRAPHIC_COMPARATOR);

    assertFullFanOut(HoodieBackedTableMetadata.routeKeyPrefixesToFileSlices(VECTOR_PARTITION, prefixes, slices), slices, prefixes);
  }

  @Test
  void nonVectorPartitionKeepsFullFanOut() {
    String partition = HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
    List<FileSlice> slices = fileSlices(partition, 8);
    // Even a key that decodes like a posting prefix must not be routed outside the vector partition.
    List<String> prefixes = Arrays.asList(VectorIndexMetadataKey.postingPrefix(GENERATION, 3, 0));

    assertFullFanOut(HoodieBackedTableMetadata.routeKeyPrefixesToFileSlices(partition, prefixes, slices), slices, prefixes);
  }

  private static String familyPrefix(int family) {
    return new String(new byte[] {(byte) family}, StandardCharsets.ISO_8859_1);
  }

  private static void assertFullFanOut(List<Pair<FileSlice, List<String>>> routed, List<FileSlice> slices, List<String> prefixes) {
    assertEquals(slices, routed.stream().map(Pair::getLeft).collect(Collectors.toList()));
    routed.forEach(sliceAndPrefixes -> assertEquals(prefixes, sliceAndPrefixes.getRight()));
  }

  private static void assertSorted(List<String> keys) {
    for (int i = 1; i < keys.size(); i++) {
      assertTrue(StringUtils.UTF8_LEXICOGRAPHIC_COMPARATOR.compare(keys.get(i - 1), keys.get(i)) < 0);
    }
  }

  /** File slices in the writer's order: sorted by file id, whose numeric suffix is the file-group index. */
  private static List<FileSlice> fileSlices(String partition, int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> new FileSlice(partition, "00000000000000000",
            HoodieTableMetadataUtil.getFileIDForFileGroup(MetadataPartitionType.fromPartitionPath(partition), i, partition, Option.empty())))
        .sorted((a, b) -> a.getFileId().compareTo(b.getFileId()))
        .collect(Collectors.toList());
  }
}
