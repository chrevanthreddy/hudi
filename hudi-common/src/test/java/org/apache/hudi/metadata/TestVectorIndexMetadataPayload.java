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

import org.apache.hudi.common.model.HoodieRecord;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestVectorIndexMetadataPayload {

  @Test
  void testPostingRecordCarriesCanonicalLookupMetadata() {
    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexPostingRecord(
        "7",
        "rk-1",
        3,
        1,
        "file-group-1",
        "dt=2026-04-01",
        "20260603120000",
        new byte[] {0x01, 0x02},
        1.5f,
        123456789L,
        "vector_index_demo");

    assertTrue(record.getData().getVectorIndexMetadata().isPresent());
    assertEquals(HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_POSTING,
        record.getData().getVectorIndexMetadata().get().getEntryType());
    assertEquals("rk-1", record.getData().getVectorIndexMetadata().get().getRecordKey());
    assertEquals(3, record.getData().getVectorIndexMetadata().get().getClusterId());
    assertEquals(1, record.getData().getVectorIndexMetadata().get().getShardId());
    assertEquals("file-group-1", record.getData().getVectorIndexMetadata().get().getFileGroupId());
    assertEquals("dt=2026-04-01", record.getData().getVectorIndexMetadata().get().getPartitionPath());
    assertEquals("20260603120000", record.getData().getVectorIndexMetadata().get().getBaseInstantTime());
  }

  @Test
  void testQuantizerMetadataRecordCarriesRaBitQConfig() {
    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexQuantizerMetadataRecord(
        "IVF_RABITQ",
        96,
        42L,
        true,
        "vector_index_demo");

    assertEquals(HoodieTableMetadataUtil.VECTOR_INDEX_QUANTIZER_KEY, record.getRecordKey());
    assertTrue(record.getData().getVectorIndexMetadata().isPresent());
    assertEquals("IVF_RABITQ", record.getData().getVectorIndexMetadata().get().getQuantizerType());
    assertEquals(96, record.getData().getVectorIndexMetadata().get().getQuantizedCodeBytes());
    assertEquals(42L, record.getData().getVectorIndexMetadata().get().getRandomSeed());
    assertTrue(record.getData().getVectorIndexMetadata().get().getAssumeNormalized());
  }
}
