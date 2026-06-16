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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.index.vector.RaBitQEncoder;
import org.apache.hudi.common.index.vector.VectorDistanceMetric;
import org.apache.hudi.common.index.vector.VectorIndexBootstrapUtils;
import org.apache.hudi.common.index.vector.VectorIndexOptions;
import org.apache.hudi.common.index.vector.VectorQuantizer;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.data.HoodieJavaRDD;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.Tuple2;

/**
 * Two-phase vector index bootstrap for Spark.
 *
 * <p>Phase 1: Sample vectors → train centroids with Spark ML KMeans. Training
 * still uses a bounded sample (FAISS, ScaNN, and Milvus all use sample-based
 * centroid training for IVF), but the iterative KMeans work stays distributed
 * instead of collecting the sample to the driver.
 *
 * <p>Phase 2: Broadcast centroids → single-pass {@code mapPartitions} that
 * assigns each vector to its nearest centroid, computes RaBitQ binary codes,
 * and emits assignment + posting + fg-mapping metadata records. Cluster
 * statistics (populations, shard counts) are gathered via a lightweight
 * {@code countByKey} before the main encoding pass.
 *
 * <p>Compared to the previous driver-local KMeans approach:
 * <ul>
 *   <li>No reflection — direct {@link HoodieMetadataPayload} method calls</li>
 *   <li>Proper {@code unpersist()} in try/finally</li>
 *   <li>generationId derived from commit instant, not wall-clock</li>
 * </ul>
 */
@Slf4j
final class SparkVectorIndexBootstrap {

  /** Hard cap for percentile-driven training samples. */
  private static final int MAX_PERCENTILE_TRAINING_SAMPLE = 10_000_000;

  /** Absolute minimum sample floor for production index quality. */
  private static final int MIN_TRAINING_SAMPLE = 1_000_000;

  /** Minimum sample multiplier per cluster. FAISS recommends 256×K minimum. */
  private static final int MIN_SAMPLE_PER_CLUSTER = 256;

  /** Lower bound for percentile-driven sampling: 0.5% of total vectors. */
  private static final double MIN_SAMPLE_PERCENT = 0.005d;

  /** Upper bound for percentile-driven sampling: 1% of total vectors. */
  private static final double MAX_SAMPLE_PERCENT = 0.01d;

  private SparkVectorIndexBootstrap() {
  }

  /**
   * Run the two-phase vector index bootstrap and return all MDT records.
   *
   * @param jsc            Spark context
   * @param vectorRows     RDD of (recordKey, partitionPath, fileId, vectorBytes) — already
   *                       read from base-table file slices
   * @param indexDef       the index definition
   * @param vectorType     resolved vector element type from the table schema
   * @param generationId   generation identifier (derived from commit instant)
   * @param lastUpdatedTs  timestamp for MDT bookkeeping
   * @return all metadata records to write into the vector index MDT partition
   */
  static HoodieData<HoodieRecord> bootstrap(JavaSparkContext jsc,
                                             JavaRDD<VectorRow> vectorRows,
                                             HoodieIndexDefinition indexDef,
                                             HoodieSchema.Vector.VectorElementType vectorType,
                                             String generationId,
                                             long lastUpdatedTs) {
    Map<String, String> options = indexDef.getIndexOptions();
    String indexName = indexDef.getIndexName();
    int dimension = VectorIndexOptions.getDimension(options);
    int numClusters = Math.max(1, VectorIndexOptions.getNumClusters(options));
    VectorDistanceMetric metric = VectorIndexOptions.getMetric(options);

    String quantizerType = VectorIndexOptions.getQuantizer(options);
    long quantizerSeed = VectorIndexOptions.getRaBitQSeed(options);
    boolean assumeNormalized = VectorIndexOptions.isRaBitQAssumeNormalized(options);
    boolean storeInMdt = "IVF_RABITQ".equals(quantizerType)
        && VectorIndexOptions.shouldStoreRaBitQCodesInMdt(options);
    int targetRowsPerShard = Math.max(1, VectorIndexOptions.getRaBitQPostingTargetRowsPerShard(options));
    int maxShardsPerCluster = Math.max(1, VectorIndexOptions.getRaBitQPostingMaxShardsPerCluster(options));
    int quantizedCodeBytes = "IVF_RABITQ".equals(quantizerType)
        ? new RaBitQEncoder(dimension, quantizerSeed, assumeNormalized).codeBytes()
        : 0;

    // ---- Phase 1: Sample → Train centroids with Spark ML KMeans ----

    vectorRows.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK());
    try {
      long totalVectors = vectorRows.count();
      if (totalVectors == 0) {
        log.warn("Vector index bootstrap found zero vectors for {}", indexName);
        return HoodieJavaRDD.of(jsc.emptyRDD());
      }

      numClusters = (int) Math.min(numClusters, totalVectors);
      int percentileTarget = (int) Math.ceil(totalVectors * MAX_SAMPLE_PERCENT);
      int percentileFloor = (int) Math.ceil(totalVectors * MIN_SAMPLE_PERCENT);
      int percentileSample = Math.max(percentileFloor, Math.min(MAX_PERCENTILE_TRAINING_SAMPLE, percentileTarget));
      int targetSample = (int) Math.min(
          totalVectors,
          Math.max(
              MIN_TRAINING_SAMPLE,
              Math.max(MIN_SAMPLE_PER_CLUSTER * numClusters, percentileSample)));
      double sampleFraction = Math.min(1.0, (double) targetSample / totalVectors);
      log.info("Vector bootstrap: {} vectors, {} clusters, sampling {} vectors ({:.2f}%) for centroid training "
              + "[policy=max(1M, 256*K, min(10M, 0.5%-1% of N))]",
          totalVectors, numClusters, targetSample, sampleFraction * 100);

      double[][] centroidsDouble = trainCentroidsWithSparkMl(
          jsc, vectorRows, dimension, numClusters, sampleFraction, totalVectors, vectorType, metric,
          VectorIndexOptions.getMaxIter(options), quantizerSeed, indexName);
      float[][] centroids = toFloatCentroids(centroidsDouble);
      int actualK = centroids.length;

      log.info("Centroid training complete: {} clusters for {}", actualK, indexName);

      // ---- Phase 2a: Assign clusters + gather stats (lightweight pass) ----

      Broadcast<float[][]> bCentroids = jsc.broadcast(centroids);
      Broadcast<VectorDistanceMetric> bMetric = jsc.broadcast(metric);
      Broadcast<Integer> bDimension = jsc.broadcast(dimension);
      Broadcast<HoodieSchema.Vector.VectorElementType> bVectorType = jsc.broadcast(vectorType);

      // Assign cluster IDs and collect (clusterId -> count) and (clusterId -> set of fileGroupIds)
      JavaPairRDD<Integer, VectorRow> assignedRows = vectorRows.mapToPair(row -> {
        float[] vector = toFloatArrayFromBytes(row.vectorBytes, bDimension.value(), bVectorType.value());
        int clusterId = findNearestCentroid(vector, bCentroids.value(), bMetric.value());
        return new Tuple2<>(clusterId, row);
      });

      assignedRows.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK());
      try {
        // Unpersist the raw vectorRows — we now have assignedRows cached
        vectorRows.unpersist();

        Map<Integer, Long> clusterVectorCounts = assignedRows.countByKey();
        Map<Integer, Integer> clusterShardCounts = new HashMap<>();
        Map<Integer, Set<String>> clusterFileGroups = new HashMap<>();

        for (Map.Entry<Integer, Long> entry : clusterVectorCounts.entrySet()) {
          clusterShardCounts.put(entry.getKey(),
              computeShardCount(entry.getValue(), targetRowsPerShard, maxShardsPerCluster));
        }

        // Gather file group IDs per cluster (lightweight collect of distinct fg IDs)
        assignedRows.mapToPair(t -> new Tuple2<>(t._1, t._2.fileId))
            .distinct()
            .groupByKey()
            .collectAsMap()
            .forEach((clusterId, fileIds) -> {
              Set<String> fgSet = new HashSet<>();
              fileIds.forEach(fgId -> {
                if (fgId != null) {
                  fgSet.add(fgId);
                }
              });
              clusterFileGroups.put(clusterId, fgSet);
            });

        log.info("Cluster stats collected: {} clusters, total {} vectors", clusterVectorCounts.size(), totalVectors);

        // ---- Phase 2b: Single-pass encode + emit canonical posting records ----

        Broadcast<Map<Integer, Integer>> bShardCounts = jsc.broadcast(clusterShardCounts);

        JavaRDD<HoodieRecord> dataRecords;
        if (storeInMdt) {
          dataRecords = buildPostingRecords(
              assignedRows, bDimension, bVectorType,
              bShardCounts, quantizerSeed, assumeNormalized, generationId, lastUpdatedTs, indexName);
        } else {
          dataRecords = jsc.emptyRDD();
        }

        // ---- Driver-side: emit singleton + cluster metadata records ----

        List<HoodieRecord> driverRecords = new ArrayList<>();

        // Centroids
        driverRecords.add(HoodieMetadataPayload.createVectorIndexCentroidsRecord(
            VectorIndexBootstrapUtils.serializeCentroids(centroidsDouble, vectorType), indexName));

        // Quantizer
        driverRecords.add(HoodieMetadataPayload.createVectorIndexQuantizerMetadataRecord(
            quantizerType, quantizedCodeBytes, quantizerSeed, assumeNormalized, indexName));

        if (storeInMdt) {
          // Manifest
          driverRecords.add(HoodieMetadataPayload.createVectorIndexManifestRecord(
              generationId, quantizerType, quantizedCodeBytes, quantizerSeed, assumeNormalized, lastUpdatedTs, indexName));

          // Generation manifest
          driverRecords.add(HoodieMetadataPayload.createVectorIndexGenerationManifestRecord(
              generationId, quantizerType, quantizedCodeBytes, quantizerSeed, assumeNormalized, lastUpdatedTs, indexName));

          // Cluster manifests
          for (Map.Entry<Integer, Long> entry : clusterVectorCounts.entrySet()) {
            int clusterId = entry.getKey();
            driverRecords.add(HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
                generationId,
                clusterId,
                clusterShardCounts.getOrDefault(clusterId, 1),
                clusterFileGroups.getOrDefault(clusterId, Collections.emptySet()),
                entry.getValue(),
                lastUpdatedTs,
                indexName));
          }
        }

        JavaRDD<HoodieRecord> driverRdd = jsc.parallelize(driverRecords, 1);

        // Flat union — no linear chain
        return HoodieJavaRDD.of(jsc.union(driverRdd, dataRecords));
      } finally {
        assignedRows.unpersist();
      }
    } finally {
      // Safety net — unpersist vectorRows if it wasn't already unpersisted
      vectorRows.unpersist(false);
    }
  }

  // ---- Record builders (no reflection!) ----

  private static double[][] trainCentroidsWithSparkMl(JavaSparkContext jsc,
                                                      JavaRDD<VectorRow> vectorRows,
                                                      int dimension,
                                                      int numClusters,
                                                      double sampleFraction,
                                                      long totalVectors,
                                                      HoodieSchema.Vector.VectorElementType vectorType,
                                                      VectorDistanceMetric metric,
                                                      int maxIter,
                                                      long seed,
                                                      String indexName) {
    JavaRDD<VectorRow> trainingRows = vectorRows;
    boolean ownsTrainingRows = false;
    if (sampleFraction < 1.0d) {
      trainingRows = vectorRows.sample(false, sampleFraction, seed);
      trainingRows.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK());
      ownsTrainingRows = true;
    }

    try {
      long trainingCount = trainingRows.count();
      if (trainingCount < numClusters && sampleFraction < 1.0d) {
        log.warn("Vector bootstrap sample for {} produced {} rows for K={}; falling back to full RDD for centroid training",
            indexName, trainingCount, numClusters);
        trainingRows.unpersist(false);
        ownsTrainingRows = false;
        trainingRows = vectorRows;
        trainingCount = totalVectors;
      }

      int actualK = (int) Math.min(numClusters, trainingCount);
      if (metric == VectorDistanceMetric.DOT_PRODUCT) {
        log.warn("Spark ML KMeans does not support dot_product distance; falling back to bounded driver-local training for {}", indexName);
        List<double[]> sampleVectors = trainingRows
            .map(row -> toDoubleArray(row.vectorBytes, dimension, vectorType))
            .collect();
        Map<String, String> trainingOptions = new HashMap<>();
        trainingOptions.put(VectorIndexOptions.DIMENSION, String.valueOf(dimension));
        trainingOptions.put(VectorIndexOptions.NUM_CLUSTERS, String.valueOf(actualK));
        trainingOptions.put(VectorIndexOptions.MAX_ITER, String.valueOf(maxIter));
        trainingOptions.put(VectorIndexOptions.METRIC, metric.name().toLowerCase());
        return VectorIndexBootstrapUtils.trainCentroids(sampleVectors, trainingOptions);
      }

      log.info("Training Spark ML KMeans centroids for {} on {} sampled vectors with K={}, maxIter={}, distanceMeasure={}",
          indexName, trainingCount, actualK, maxIter, sparkMlDistanceMeasure(metric));
      SparkSession spark = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate();
      JavaRDD<Row> featureRows = trainingRows.map(row ->
          RowFactory.create(Vectors.dense(toDoubleArray(row.vectorBytes, dimension, vectorType))));
      StructType schema = new StructType(new StructField[] {
          new StructField("features", new VectorUDT(), false, Metadata.empty())
      });
      Dataset<Row> trainingDf = spark.createDataFrame(featureRows, schema);
      KMeansModel model = new KMeans()
          .setK(actualK)
          .setMaxIter(Math.max(1, maxIter))
          .setSeed(seed)
          .setFeaturesCol("features")
          .setPredictionCol("prediction")
          .setDistanceMeasure(sparkMlDistanceMeasure(metric))
          .fit(trainingDf);
      org.apache.spark.ml.linalg.Vector[] centers = model.clusterCenters();
      double[][] centroids = new double[centers.length][];
      for (int i = 0; i < centers.length; i++) {
        centroids[i] = centers[i].toArray();
      }
      return centroids;
    } finally {
      if (ownsTrainingRows) {
        trainingRows.unpersist(false);
      }
    }
  }

  private static String sparkMlDistanceMeasure(VectorDistanceMetric metric) {
    switch (metric) {
      case COSINE:
        return "cosine";
      case L2:
        return "euclidean";
      default:
        throw new IllegalArgumentException("Spark ML KMeans does not support metric: " + metric);
    }
  }

  private static JavaRDD<HoodieRecord> buildPostingRecords(
      JavaPairRDD<Integer, VectorRow> assignedRows,
      Broadcast<Integer> bDimension,
      Broadcast<HoodieSchema.Vector.VectorElementType> bVectorType,
      Broadcast<Map<Integer, Integer>> bShardCounts,
      long quantizerSeed,
      boolean assumeNormalized,
      String generationId,
      long lastUpdatedTs,
      String indexName) {
    return assignedRows.mapPartitions(iterator -> {
      RaBitQEncoder encoder = null;
      List<HoodieRecord> records = new ArrayList<>();
      while (iterator.hasNext()) {
        Tuple2<Integer, VectorRow> entry = iterator.next();
        int clusterId = entry._1;
        VectorRow row = entry._2;
        int shardCount = bShardCounts.value().getOrDefault(clusterId, 1);
        int shardId = computeShardId(row.recordKey, shardCount);

        // Posting record with RaBitQ encoding
        if (encoder == null) {
          encoder = new RaBitQEncoder(bDimension.value(), quantizerSeed, assumeNormalized);
        }
        float[] vector = toFloatArrayFromBytes(row.vectorBytes, bDimension.value(), bVectorType.value());
        VectorQuantizer.QuantizedVector quantized = encoder.encode(vector);
        Float scalar = assumeNormalized ? null : quantized.scalar;
        records.add(HoodieMetadataPayload.createVectorIndexPostingRecord(
            generationId, row.recordKey, clusterId, shardId,
            row.fileId, row.partitionPath, row.baseInstantTime,
            quantized.code, scalar, lastUpdatedTs, indexName));
      }
      return records.iterator();
    });
  }

  // ---- Vector conversion ----

  /**
   * Convert raw vector bytes to float array. This handles FLOAT, DOUBLE, and INT8 element types
   * where the backing storage is a fixed-size byte array (the primary production path).
   */
  static float[] toFloatArrayFromBytes(byte[] vectorBytes, int dimension,
                                       HoodieSchema.Vector.VectorElementType elementType) {
    ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    float[] result = new float[dimension];
    switch (elementType) {
      case FLOAT:
        for (int i = 0; i < dimension; i++) {
          result[i] = buffer.getFloat();
        }
        return result;
      case DOUBLE:
        for (int i = 0; i < dimension; i++) {
          result[i] = (float) buffer.getDouble();
        }
        return result;
      case INT8:
        for (int i = 0; i < dimension; i++) {
          result[i] = buffer.get();
        }
        return result;
      default:
        throw new IllegalArgumentException("Unsupported vector element type: " + elementType);
    }
  }

  /**
   * Convert raw vector bytes to double array for centroid training.
   */
  static double[] toDoubleArray(byte[] vectorBytes, int dimension,
                                HoodieSchema.Vector.VectorElementType elementType) {
    ByteBuffer buffer = ByteBuffer.wrap(vectorBytes).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    double[] result = new double[dimension];
    switch (elementType) {
      case FLOAT:
        for (int i = 0; i < dimension; i++) {
          result[i] = buffer.getFloat();
        }
        return result;
      case DOUBLE:
        for (int i = 0; i < dimension; i++) {
          result[i] = buffer.getDouble();
        }
        return result;
      case INT8:
        for (int i = 0; i < dimension; i++) {
          result[i] = buffer.get();
        }
        return result;
      default:
        throw new IllegalArgumentException("Unsupported vector element type: " + elementType);
    }
  }

  // ---- Centroid math ----

  static int findNearestCentroid(float[] vector, float[][] centroids, VectorDistanceMetric metric) {
    int best = 0;
    float bestDist = metric.compute(vector, centroids[0]);
    for (int i = 1; i < centroids.length; i++) {
      float dist = metric.compute(vector, centroids[i]);
      if (dist < bestDist) {
        bestDist = dist;
        best = i;
      }
    }
    return best;
  }

  // ---- Utilities ----

  private static int computeShardCount(long clusterPopulation, int targetRowsPerShard, int maxShardsPerCluster) {
    if (clusterPopulation <= 0) {
      return 1;
    }
    long computed = (clusterPopulation + targetRowsPerShard - 1L) / targetRowsPerShard;
    return (int) Math.min(Math.max(1L, computed), maxShardsPerCluster);
  }

  static int computeShardId(String recordKey, int shardCount) {
    // Murmur3-style bit mixing for better distribution than String.hashCode()
    int h = recordKey.hashCode();
    h ^= (h >>> 16);
    h *= 0x85ebca6b;
    h ^= (h >>> 13);
    return Math.floorMod(h, Math.max(1, shardCount));
  }

  private static float[][] toFloatCentroids(double[][] centroids) {
    float[][] result = new float[centroids.length][];
    for (int i = 0; i < centroids.length; i++) {
      result[i] = new float[centroids[i].length];
      for (int j = 0; j < centroids[i].length; j++) {
        result[i][j] = (float) centroids[i][j];
      }
    }
    return result;
  }

  // ---- Data carrier ----

  /**
   * Lightweight serializable carrier for vector data read from base-table file slices.
   * Avoids the overhead of Spark Rows, DataFrames, and InternalRow wrappers.
   */
  static final class VectorRow implements Serializable {
    private static final long serialVersionUID = 2L;

    final String recordKey;
    final String partitionPath;
    final String fileId;
    final String baseInstantTime;
    final byte[] vectorBytes;

    VectorRow(String recordKey, String partitionPath, String fileId, String baseInstantTime, byte[] vectorBytes) {
      this.recordKey = recordKey;
      this.partitionPath = partitionPath;
      this.fileId = fileId;
      this.baseInstantTime = baseInstantTime;
      this.vectorBytes = vectorBytes;
    }
  }
}
