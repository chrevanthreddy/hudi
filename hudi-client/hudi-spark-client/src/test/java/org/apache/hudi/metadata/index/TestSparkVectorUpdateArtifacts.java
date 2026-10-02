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

package org.apache.hudi.metadata.index;

import org.apache.hudi.avro.model.HoodieVectorIndexManifest;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.RawKey;
import org.apache.hudi.metadata.SparkVectorIndexUpdater;
import org.apache.hudi.metadata.VectorIndexMetadataKey;
import org.apache.hudi.metadata.VectorRoutingArtifacts;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestSparkVectorUpdateArtifacts {
  private static final String PARTITION = "vector_index_embedding";
  private static final int GENERATION = 1;
  private static final HoodieSchema.Vector VECTOR =
      HoodieSchema.createVector(2, HoodieSchema.Vector.VectorElementType.FLOAT);

  @Test
  void loadsPersistedTwoLevelPlacementAndRejectsDigestMismatch() {
    Map<String, HoodieRecord<HoodieMetadataPayload>> artifacts = artifactRecords();
    HoodieTableMetadata metadata = metadata(artifacts);
    SparkIndexerSupport support = new SparkIndexerSupport(null, null);

    SparkVectorIndexUpdater.Artifacts loaded = support.loadVectorArtifacts(metadata, PARTITION, GENERATION, VECTOR);
    assertNotNull(loaded);

    HoodieVectorIndexManifest manifest = (HoodieVectorIndexManifest) artifacts
        .get(VectorIndexMetadataKey.manifest(GENERATION)).getData().getVectorIndexMetadata().get();
    manifest.setRoutingArtifactDigest("mismatched");
    assertThrows(HoodieMetadataException.class,
        () -> support.loadVectorArtifacts(metadata, PARTITION, GENERATION, VECTOR));
  }

  @Test
  void rejectsMissingCentroidsAndLegacyFlatRouting() {
    Map<String, HoodieRecord<HoodieMetadataPayload>> missing = artifactRecords();
    SparkIndexerSupport support = new SparkIndexerSupport(null, null);
    missing.remove(VectorIndexMetadataKey.centroids(GENERATION, 0));
    assertThrows(HoodieMetadataException.class,
        () -> support.loadVectorArtifacts(metadata(missing), PARTITION, GENERATION, VECTOR));

    Map<String, HoodieRecord<HoodieMetadataPayload>> legacy = artifactRecords();
    HoodieVectorIndexManifest manifest = (HoodieVectorIndexManifest) legacy
        .get(VectorIndexMetadataKey.manifest(GENERATION)).getData().getVectorIndexMetadata().get();
    manifest.setRoutingVersion(null);
    assertThrows(HoodieMetadataException.class,
        () -> support.loadVectorArtifacts(metadata(legacy), PARTITION, GENERATION, VECTOR));
  }

  @Test
  void rejectsMalformedCentroidPayloadAndShardGeometry() {
    SparkIndexerSupport support = new SparkIndexerSupport(null, null);
    Map<String, HoodieRecord<HoodieMetadataPayload>> malformed = artifactRecords();
    HoodieRecord<HoodieMetadataPayload> shortCentroids = HoodieMetadataPayload.createVectorIndexCentroidsRecord(
        GENERATION, 0, ByteBuffer.allocate(2 * Integer.BYTES), ByteBuffer.allocate(1),
        ByteBuffer.allocate(0), PARTITION);
    malformed.put(shortCentroids.getRecordKey(), shortCentroids);
    assertThrows(HoodieMetadataException.class,
        () -> support.loadVectorArtifacts(metadata(malformed), PARTITION, GENERATION, VECTOR));

    Map<String, HoodieRecord<HoodieMetadataPayload>> invalidShard = artifactRecords();
    HoodieVectorIndexManifest manifest = (HoodieVectorIndexManifest) invalidShard
        .get(VectorIndexMetadataKey.manifest(GENERATION)).getData().getVectorIndexMetadata().get();
    manifest.setShardCount(0);
    assertThrows(HoodieMetadataException.class,
        () -> support.loadVectorArtifacts(metadata(invalidShard), PARTITION, GENERATION, VECTOR));
  }

  private static HoodieTableMetadata metadata(Map<String, HoodieRecord<HoodieMetadataPayload>> records) {
    HoodieTableMetadata metadata = mock(HoodieTableMetadata.class);
    when(metadata.getRecordsByKeyPrefixes(any(), eq(PARTITION), eq(true)))
        .thenAnswer(invocation -> {
          HoodieData<? extends RawKey> keys = invocation.getArgument(0);
          return HoodieListData.eager(keys.map(RawKey::encode).collectAsList().stream()
              .map(records::get).filter(record -> record != null).collect(java.util.stream.Collectors.toList()));
        });
    return metadata;
  }

  private static Map<String, HoodieRecord<HoodieMetadataPayload>> artifactRecords() {
    float[][] leaves = {{100.0f, 0.0f}, {11.0f, 0.0f}};
    float[][] coarse = {{0.0f, 0.0f}, {10.0f, 0.0f}};
    int[] offsets = {0, 1, 2};
    HoodieRecord<HoodieMetadataPayload> manifest = HoodieMetadataPayload.createVectorIndexManifestRecord(
        GENERATION, "1", "ACTIVE", 2, 2, 1, 1, 0, 2, 1, 2, "L2",
        false, false, "embedding", 524288, 1024, 1, 1,
        0.0, 0.0, 0.0, 0.0, 1, "centroids", 1, 1, "000", "000", 1L, PARTITION);
    HoodieMetadataPayload.createVectorIndexRoutedManifestRecord(
        manifest, VectorRoutingArtifacts.ROUTING_VERSION,
        serializeFloats(coarse), serializeInts(offsets), 1.1f,
        VectorRoutingArtifacts.digest(coarse, leaves, offsets, 1.1f));
    ByteBuffer ids = ByteBuffer.allocate(2 * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    ids.putInt(0).putInt(1).flip();
    ByteBuffer values = ByteBuffer.allocate(4 * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    Arrays.stream(leaves).forEach(row -> {
      for (float value : row) {
        values.putFloat(value);
      }
    });
    values.flip();
    Map<String, HoodieRecord<HoodieMetadataPayload>> records = new HashMap<>();
    records.put(manifest.getRecordKey(), manifest);
    HoodieRecord<HoodieMetadataPayload> centroids = HoodieMetadataPayload.createVectorIndexCentroidsRecord(
        GENERATION, 0, ids, values, ByteBuffer.allocate(0), PARTITION);
    records.put(centroids.getRecordKey(), centroids);
    HoodieRecord<HoodieMetadataPayload> quantizer = HoodieMetadataPayload.createVectorIndexQuantizerMetadataRecord(
        GENERATION, 0, "IVF_RABITQ", 42L, null, PARTITION);
    records.put(quantizer.getRecordKey(), quantizer);
    return records;
  }

  private static ByteBuffer serializeFloats(float[][] rows) {
    ByteBuffer result = ByteBuffer.allocate(rows.length * rows[0].length * Float.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN);
    for (float[] row : rows) {
      for (float value : row) {
        result.putFloat(value);
      }
    }
    result.flip();
    return result;
  }

  private static ByteBuffer serializeInts(int[] values) {
    ByteBuffer result = ByteBuffer.allocate(values.length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int value : values) {
      result.putInt(value);
    }
    result.flip();
    return result;
  }
}
