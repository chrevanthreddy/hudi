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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metadata.index.vector;

import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieIndexMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.VectorIndexMetadataKey;
import org.apache.hudi.metadata.index.EngineIndexerSupport;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TestVectorIndexer {

  private static final String INDEX_PARTITION = "vector_index_embedding";
  private static final String BASELINE = "000";
  private static final String INSTANT = "002";
  private static final int GENERATION = 3;

  @Test
  void testNoOpCommitStillDispatchesUpdateAndEmitsSourceMarker() {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexDefinition indexDefinition = mock(HoodieIndexDefinition.class);
    HoodieBackedTableMetadata tableMetadata = mock(HoodieBackedTableMetadata.class);
    EngineIndexerSupport engineSupport = mock(EngineIndexerSupport.class);
    HoodieSchema tableSchema = mock(HoodieSchema.class);

    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getMetadataPartitions()).thenReturn(Collections.singleton(INDEX_PARTITION));
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(
        Collections.singletonMap(INDEX_PARTITION, indexDefinition));
    when(indexDefinition.getIndexName()).thenReturn(INDEX_PARTITION);
    when(tableMetadata.getRecordsByKeyPrefixes(any(), any(), anyBoolean()))
        .thenReturn(HoodieListData.eager(List.of(
            HoodieMetadataPayload.createVectorIndexActiveManifestRecord(
                GENERATION, INDEX_PARTITION),
            currentManifestRecord())));
    when(engineSupport.generateVectorIndexUpdateRecords(
        any(), any(), any(), any(), any(), anyInt(), any()))
        .thenReturn(HoodieListData.eager(Collections.emptyList()));

    try (MockedStatic<HoodieTableMetadataUtil> metadataUtil = mockStatic(HoodieTableMetadataUtil.class);
         MockedConstruction<TableSchemaResolver> ignored = mockConstruction(
             TableSchemaResolver.class,
             (resolver, context) -> when(resolver.getTableSchema()).thenReturn(tableSchema))) {
      metadataUtil.when(() -> HoodieTableMetadataUtil.getHoodieIndexDefinition(
          INDEX_PARTITION, metaClient)).thenReturn(indexDefinition);

      VectorIndexer indexer = new VectorIndexer(
          engineContext, writeConfig, metaClient, engineSupport);
      List<IndexPartitionAndRecords> updates = indexer.buildUpdate(IndexUpdateContext.of(
          INSTANT,
          tableMetadata,
          Lazy.lazily(() -> mock(HoodieTableFileSystemView.class)),
          new HoodieCommitMetadata()));

      assertEquals(1, updates.size());
      assertEquals(INDEX_PARTITION, updates.get(0).indexPartitionName());
      List<HoodieRecord> records = updates.get(0).indexRecords().collectAsList();
      assertEquals(1, records.size());
      assertEquals(
          VectorIndexMetadataKey.sourceInstantMarker(GENERATION, INSTANT),
          records.get(0).getRecordKey());
      verify(engineSupport).generateVectorIndexUpdateRecords(
          indexDefinition, metaClient, tableMetadata, Collections.emptyList(),
          tableSchema, GENERATION, INSTANT);
    }
  }

  @ParameterizedTest
  @EnumSource(value = WriteOperationType.class, names = {"COMPACT", "LOG_COMPACT"})
  void testCompactionSkipsPostingWorkButEmitsSourceMarker(WriteOperationType operationType) {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexDefinition indexDefinition = mock(HoodieIndexDefinition.class);
    HoodieBackedTableMetadata tableMetadata = mock(HoodieBackedTableMetadata.class);
    EngineIndexerSupport engineSupport = mock(EngineIndexerSupport.class);
    HoodieTableFileSystemView fsView = mock(HoodieTableFileSystemView.class);

    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getMetadataPartitions()).thenReturn(Collections.singleton(INDEX_PARTITION));
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(
        Collections.singletonMap(INDEX_PARTITION, indexDefinition));
    when(indexDefinition.getIndexName()).thenReturn(INDEX_PARTITION);
    when(tableMetadata.getRecordsByKeyPrefixes(any(), any(), anyBoolean()))
        .thenReturn(HoodieListData.eager(List.of(
            HoodieMetadataPayload.createVectorIndexActiveManifestRecord(GENERATION, INDEX_PARTITION),
            currentManifestRecord())));

    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath("p1");
    stat.setFileId("fg-1");
    stat.setPath("p1/fg-1_0-1-2_" + INSTANT + ".parquet");
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(operationType);
    commitMetadata.addWriteStat("p1", stat);

    try (MockedStatic<HoodieTableMetadataUtil> metadataUtil = mockStatic(HoodieTableMetadataUtil.class)) {
      metadataUtil.when(() -> HoodieTableMetadataUtil.getHoodieIndexDefinition(
          INDEX_PARTITION, metaClient)).thenReturn(indexDefinition);

      List<IndexPartitionAndRecords> updates = new VectorIndexer(
          engineContext, mock(HoodieWriteConfig.class), metaClient, engineSupport)
          .buildUpdate(IndexUpdateContext.of(INSTANT, tableMetadata, Lazy.lazily(() -> fsView), commitMetadata));

      List<HoodieRecord> records = updates.get(0).indexRecords().collectAsList();
      assertEquals(1, records.size());
      assertEquals(VectorIndexMetadataKey.sourceInstantMarker(GENERATION, INSTANT), records.get(0).getRecordKey());
      verifyNoInteractions(engineSupport, fsView);
    }
  }

  @ParameterizedTest
  @EnumSource(value = WriteOperationType.class, names = {"CLUSTER", "UPSERT"})
  void testOnlyClusteringMarksFileGroupsAsRelocations(WriteOperationType operationType) {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieIndexMetadata indexMetadata = mock(HoodieIndexMetadata.class);
    HoodieIndexDefinition indexDefinition = mock(HoodieIndexDefinition.class);
    HoodieBackedTableMetadata tableMetadata = mock(HoodieBackedTableMetadata.class);
    EngineIndexerSupport engineSupport = mock(EngineIndexerSupport.class);
    HoodieSchema tableSchema = mock(HoodieSchema.class);
    HoodieTableFileSystemView fsView = mock(HoodieTableFileSystemView.class);

    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/table"));
    when(tableConfig.getMetadataPartitions()).thenReturn(Collections.singleton(INDEX_PARTITION));
    when(metaClient.getIndexMetadata()).thenReturn(Option.of(indexMetadata));
    when(indexMetadata.getIndexDefinitions()).thenReturn(
        Collections.singletonMap(INDEX_PARTITION, indexDefinition));
    when(indexDefinition.getIndexName()).thenReturn(INDEX_PARTITION);
    when(tableMetadata.getRecordsByKeyPrefixes(any(), any(), anyBoolean()))
        .thenReturn(HoodieListData.eager(List.of(
            HoodieMetadataPayload.createVectorIndexActiveManifestRecord(GENERATION, INDEX_PARTITION),
            currentManifestRecord())));
    when(fsView.getLatestMergedFileSliceBeforeOrOn("p1", INSTANT, "fg-new")).thenReturn(Option.empty());
    when(engineSupport.generateVectorIndexUpdateRecords(
        any(), any(), any(), any(), any(), anyInt(), any()))
        .thenReturn(HoodieListData.eager(Collections.emptyList()));

    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath("p1");
    stat.setFileId("fg-new");
    stat.setPath("p1/fg-new_0-1-2_" + INSTANT + ".parquet");
    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();
    commitMetadata.setOperationType(operationType);
    commitMetadata.addWriteStat("p1", stat);

    try (MockedStatic<HoodieTableMetadataUtil> metadataUtil = mockStatic(HoodieTableMetadataUtil.class);
         MockedConstruction<TableSchemaResolver> ignored = mockConstruction(
             TableSchemaResolver.class,
             (resolver, context) -> when(resolver.getTableSchema()).thenReturn(tableSchema))) {
      metadataUtil.when(() -> HoodieTableMetadataUtil.getHoodieIndexDefinition(
          INDEX_PARTITION, metaClient)).thenReturn(indexDefinition);

      new VectorIndexer(engineContext, writeConfig, metaClient, engineSupport)
          .buildUpdate(IndexUpdateContext.of(INSTANT, tableMetadata, Lazy.lazily(() -> fsView), commitMetadata));

      ArgumentCaptor<List<VectorIndexFileGroupUpdate>> updates = ArgumentCaptor.forClass(List.class);
      verify(engineSupport).generateVectorIndexUpdateRecords(
          any(), any(), any(), updates.capture(), any(), anyInt(), any());
      assertEquals(1, updates.getValue().size());
      assertEquals(operationType == WriteOperationType.CLUSTER, updates.getValue().get(0).relocatesExistingRows());
    }
  }

  private static HoodieRecord currentManifestRecord() {
    return manifestRecord(INSTANT);
  }

  private static HoodieRecord manifestRecord(String lastCoveredInstant) {
    return HoodieMetadataPayload.createVectorIndexManifestRecord(
        GENERATION, Integer.toString(GENERATION), "ACTIVE",
        2, 2, 1, 1, 0, 1,
        1, 1, "L2", false, false, "embedding",
        65536, 100, 1, 1, 0.0, 0.0, 0.0, 0.0,
        1, "checksum", 2, 1, BASELINE, lastCoveredInstant, 0L, INDEX_PARTITION);
  }
}
