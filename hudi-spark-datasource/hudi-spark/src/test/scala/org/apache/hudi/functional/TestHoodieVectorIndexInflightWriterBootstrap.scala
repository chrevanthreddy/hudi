/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.avro.model.HoodieVectorIndexManifest
import org.apache.hudi.client.{SparkRDDWriteClient, WriteStatus}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.validator.SparkPreCommitValidator
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.{HoodieData, HoodieListData}
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.index.vector.VectorIndexMetadataCache
import org.apache.hudi.common.model.{HoodieAvroPayload, HoodieKey, HoodieRecord, WriteOperationType}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.TableNotFoundException
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieMetadataPayload, RawKey, VectorIndexMetadataKey, VectorMetadataRawKey}
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.avro.generic.GenericRecord
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType, MetadataBuilder, StringType, StructField, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import java.util.Collections
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.collection.JavaConverters._

/**
 * Reviewer scenario on #20107: writer A requests t3 and is still inflight when the vector index is
 * built after writer B completed t4. Like RLI/SI, A must index itself when it commits, and the
 * bootstrap baseline stays fixed; strict read validation checks the committed source markers.
 */
class TestHoodieVectorIndexInflightWriterBootstrap extends HoodieSparkClientTestBase {

  private val IndexName = "vec_inflight"
  private val IndexPartition = s"vector_index_$IndexName"
  private val Generation = 1
  private val NumClusters = 2

  private var spark: SparkSession = _

  @BeforeEach
  override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    // Multi-writer settings for every writer in this test, including CREATE INDEX: LAZY failed-write
    // cleaning keeps the index writer from rolling back writer A's inflight instant.
    spark.sql("set hoodie.write.lock.provider = org.apache.hudi.client.transaction.lock.InProcessLockProvider")
    spark.sql("set hoodie.write.concurrency.mode = optimistic_concurrency_control")
    spark.sql("set hoodie.clean.failed.writes.policy = LAZY")
    initHoodieStorage()
    BlockingPreCommitValidator.reset()
  }

  @AfterEach
  override def tearDown(): Unit = {
    BlockingPreCommitValidator.release.countDown()
    cleanupSparkContexts()
    cleanupFileSystem()
  }

  @Test
  def testWriterInflightDuringBootstrapIsCoveredWhenItCompletes(): Unit = {
    val tableName = "vector_inflight_writer"
    val tablePath = s"$basePath/$tableName"
    createTable(tableName, tablePath)
    writeRows(tablePath, seedRows, writeOptions(tableName))

    val pool = Executors.newFixedThreadPool(2)
    try {
      // Writer A (t3): parks in pre-commit validation with its instant still inflight.
      val writerA = pool.submit(new Runnable {
        override def run(): Unit = writeRows(tablePath, Seq(Row("a-1", 1L, Seq(10.1f, 10.1f), "a")),
          writeOptions(tableName) + ("hoodie.precommit.validators" -> classOf[BlockingPreCommitValidator].getName))
      })
      assertTrue(BlockingPreCommitValidator.entered.await(120, TimeUnit.SECONDS), "writer A never reached pre-commit")
      val instantA = latestWriteInstant(tablePath, completed = false)

      // Writer B (t4) completes while A is still inflight.
      writeRows(tablePath, Seq(Row("b-1", 1L, Seq(9.9f, 9.9f), "b")), writeOptions(tableName))
      val instantB = latestWriteInstant(tablePath, completed = true)
      assertTrue(instantA < instantB, s"fixture expects A ($instantA) to precede B ($instantB)")

      // Build the index while A is inflight; the indexer's catch-up then waits for A.
      val createIndex = pool.submit(new Runnable {
        override def run(): Unit = spark.sql(
          s"""
             |CREATE INDEX $IndexName ON $tableName
             |USING VECTOR (embedding)
             |OPTIONS (
             |  `vector.num_clusters` = '$NumClusters',
             |  `vector.query.nprobes` = '$NumClusters',
             |  `vector.metric` = 'L2',
             |  `vector.max_iter` = '10',
             |  `vector.rabitq.seed` = '42'
             |)
             |""".stripMargin)
      })
      // Observe inflight marking before bootstrap clears it when enabling the built partition.
      awaitIndexPartitionInflight(tablePath)
      awaitIndexPartitionBuilt(tablePath)
      val bootstrap = manifest(tablePath)
      assertTrue(bootstrap.getVerifiedFrontier.toString < instantA,
        s"bootstrap frontier ${bootstrap.getVerifiedFrontier} must not cover inflight writer A ($instantA)")

      // Release after the inflight partition is visible in table config; releasing on the scheduler
      // latch alone races the config rewrite. awaitIndexPartitionBuilt also waits for its backup
      // to disappear, so observing the new primary cannot release A before that rewrite is closed.
      // The final index-completion rewrite is blocked by catch-up until A commits.
      BlockingPreCommitValidator.release.countDown()
      writerA.get(300, TimeUnit.SECONDS)
      createIndex.get(300, TimeUnit.SECONDS)

      assertTrue(hasSourceMarker(tablePath, instantA), s"writer A ($instantA) did not index itself on commit")
      assertEquals(bootstrap.getBootstrapInstant, manifest(tablePath).getBootstrapInstant,
        "writes must preserve the bootstrap baseline")

      spark.catalog.refreshTable(tableName)
      val bruteForce = search(tableName, "brute_force", "")
      val exact = search(tableName, "ivf_rabitq_mdt",
        s"vector.query.nprobes=$NumClusters,vector.query.mode=exact_rerank,vector.freshness.strict=true,vector.freshness.policy=fail")
      assertEquals(bruteForce, exact)
      assertTrue(exact.contains("a-1") && exact.contains("b-1"), s"exact results missed A or B: $exact")
    } finally {
      BlockingPreCommitValidator.release.countDown()
      pool.shutdownNow()
      assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS),
        "writer/index worker still alive before Spark and filesystem teardown")
    }
  }

  @Test
  def testClientCreatedBeforeIndexRefreshesMetadataOnWrite(): Unit = {
    val tableName = "vector_cached_client"
    val tablePath = s"$basePath/$tableName"
    createTable(tableName, tablePath)
    writeRows(tablePath, seedRows, writeOptions(tableName))
    val config = HoodieWriteConfig.newBuilder().withPath(tablePath).forTable(tableName)
      .withSchema(new TableSchemaResolver(metaClientFor(tablePath)).getTableSchema(false).toString)
      .withProps(writeOptions(tableName).asJava).build()
    val client = new SparkRDDWriteClient[HoodieAvroPayload](new HoodieSparkEngineContext(jsc), config)
    try {
      val before = client.initTable(WriteOperationType.UNKNOWN, HOption.empty[String]())
      assertTrue(!before.getMetaClient.getTableConfig.getMetadataPartitions.contains(IndexPartition))
      spark.sql(s"CREATE INDEX $IndexName ON $tableName USING VECTOR (embedding) " +
        s"OPTIONS (`vector.num_clusters`='$NumClusters', `vector.query.nprobes`='$NumClusters')")
      val instant = client.startCommit()
      val statuses = client.delete(jsc.parallelize(Collections.singletonList(new HoodieKey("left-1", ""))), instant)
      assertTrue(statuses.collect().asScala.forall(!_.hasErrors))
      assertTrue(client.commit(instant, statuses))
      assertEquals(instant, latestWriteInstant(tablePath, completed = true))
      assertTrue(hasSourceMarker(tablePath, instant),
        "a pre-existing current client must refresh metadata and run the vector hook, not manufacture a legacy gap")
      assertEquals(search(tableName, "brute_force", ""), search(tableName, "ivf_rabitq_mdt",
        s"vector.query.nprobes=$NumClusters,vector.freshness.strict=true,vector.freshness.policy=fail"))
    } finally {
      client.close()
    }
  }

  private def awaitIndexPartitionInflight(tablePath: String): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(300)
    def markedInflight: Boolean =
      try metaClientFor(tablePath).getTableConfig.getMetadataPartitionsInflight.contains(IndexPartition)
      catch { case _: TableNotFoundException => false }
    while (!markedInflight) {
      assertTrue(System.nanoTime() < deadline, "vector index partition was never marked inflight")
      Thread.sleep(10)
    }
  }

  private def awaitIndexPartitionBuilt(tablePath: String): Unit = {
    val deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(300)
    while (!indexPartitionBuilt(tablePath)) {
      assertTrue(System.currentTimeMillis() < deadline, "vector index partition was never built")
      Thread.sleep(500)
    }
  }

  // CREATE INDEX rewrites hoodie.properties concurrently; a transiently unreadable config is "not yet".
  private def indexPartitionBuilt(tablePath: String): Boolean =
    try {
      val metaClient = metaClientFor(tablePath)
      metaClient.getTableConfig.getMetadataPartitions.contains(IndexPartition) &&
        !metaClient.getStorage.exists(new org.apache.hudi.storage.StoragePath(
          metaClient.getMetaPath, "hoodie.properties.backup"))
    } catch { case _: TableNotFoundException => false }

  private def createTable(tableName: String, tablePath: String): Unit =
    spark.sql(
      s"""
         |CREATE TABLE $tableName (id STRING, ts BIGINT, embedding VECTOR(2), label STRING) USING hudi
         |OPTIONS (
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  type = 'COPY_ON_WRITE',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.global.record.level.index.enable = 'true'
         |)
         |LOCATION '$tablePath'
         |""".stripMargin)

  // Small-file packing is disabled so A and B write disjoint file groups and OCC sees no conflict.
  private def writeOptions(tableName: String): Map[String, String] = Map(
    TABLE_NAME.key -> tableName,
    TABLE_TYPE.key -> "COPY_ON_WRITE",
    RECORDKEY_FIELD.key -> "id",
    PRECOMBINE_FIELD.key -> "ts",
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true",
    HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key -> "optimistic_concurrency_control",
    "hoodie.clean.failed.writes.policy" -> "LAZY",
    "hoodie.write.lock.provider" -> "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
    "hoodie.parquet.small.file.limit" -> "0")

  private def seedRows: Seq[Row] = Seq(
    Row("left-1", 1L, Seq(-10.0f, -9.8f), "left"),
    Row("left-2", 1L, Seq(-9.7f, -10.0f), "left"),
    Row("right-1", 1L, Seq(10.0f, 9.8f), "right"),
    Row("right-2", 1L, Seq(9.7f, 10.0f), "right"))

  private def writeRows(tablePath: String, rows: Seq[Row], options: Map[String, String]): Unit = {
    val metadata = new MetadataBuilder().putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(2)").build()
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("ts", LongType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, metadata),
      StructField("label", StringType, nullable = false)))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .write.format("hudi").options(options).mode(SaveMode.Append).save(tablePath)
  }

  private def latestWriteInstant(tablePath: String, completed: Boolean): String = {
    val writes = metaClientFor(tablePath).getActiveTimeline.getWriteTimeline
    val filtered = if (completed) writes.filterCompletedInstants else writes.filterInflightsAndRequested
    filtered.lastInstant.get.requestedTime
  }

  private def search(tableName: String, algorithm: String, runtimeOptions: String): Seq[String] = {
    val optionArg = if (runtimeOptions.isEmpty) "" else s", '$runtimeOptions'"
    spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search('$tableName', 'embedding', ARRAY(10.0, 10.0), 6, 'l2', '$algorithm'$optionArg)
         |""".stripMargin)
      .collect()
      .map(row => row.getAs[String]("id") -> row.getAs[Double]("_hudi_distance"))
      .sortBy { case (key, distance) => (distance, key) }
      .map(_._1)
      .toSeq
  }

  private def manifest(tablePath: String): HoodieVectorIndexManifest = withMetadataTable(tablePath) { metadataTable =>
    val manifests = records(metadataTable, VectorIndexMetadataKey.manifest(Generation))
      .flatMap(record => Option(record.getData.getVectorIndexMetadata.orElse(null)).map(_.asInstanceOf[AnyRef]))
      .filter(value => schemaName(value) == "HoodieVectorIndexManifest")
      .map(value => VectorIndexMetadataCache.asManifest(value))
    assertEquals(1, manifests.size)
    manifests.head
  }

  private def hasSourceMarker(tablePath: String, instant: String): Boolean = withMetadataTable(tablePath) { metadataTable =>
    records(metadataTable, VectorIndexMetadataKey.sourceInstantMarker(Generation, instant)).nonEmpty
  }

  private def records(metadataTable: HoodieBackedTableMetadata, exactKey: String): Seq[HoodieRecord[HoodieMetadataPayload]] = {
    val key: RawKey = new VectorMetadataRawKey(exactKey)
    metadataTable.getRecordsByKeyPrefixes(HoodieListData.eager(Collections.singletonList(key)), IndexPartition, true)
      .collectAsList().asScala.toSeq
  }

  private def schemaName(value: AnyRef): String = value match {
    case generic: GenericRecord => generic.getSchema.getName
    case specific => specific.getClass.getSimpleName
  }

  private def withMetadataTable[T](tablePath: String)(f: HoodieBackedTableMetadata => T): T = {
    val config = HoodieMetadataConfig.newBuilder().enable(true).build()
    val context = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
    val metadataTable = new HoodieBackedTableMetadata(context, metaClientFor(tablePath).getStorage, config, tablePath)
    try f(metadataTable) finally metadataTable.close()
  }

  private def metaClientFor(tablePath: String): HoodieTableMetaClient =
    HoodieTestUtils.createMetaClient(storageConf, tablePath)
}

/** Holds a writer's instant inflight inside pre-commit validation until the test releases it. */
class BlockingPreCommitValidator(table: HoodieSparkTable[_], context: HoodieEngineContext, config: HoodieWriteConfig)
  extends SparkPreCommitValidator[AnyRef, AnyRef, AnyRef, HoodieData[WriteStatus]](
    table.asInstanceOf[HoodieSparkTable[AnyRef]], context, config) {

  override protected def validateRecordsBeforeAndAfter(
      before: Dataset[Row], after: Dataset[Row], partitionsAffected: java.util.Set[String]): Unit = {
    BlockingPreCommitValidator.entered.countDown()
    BlockingPreCommitValidator.release.await(300, TimeUnit.SECONDS)
  }
}

object BlockingPreCommitValidator {
  @volatile var entered: CountDownLatch = new CountDownLatch(1)
  @volatile var release: CountDownLatch = new CountDownLatch(1)

  def reset(): Unit = {
    entered = new CountDownLatch(1)
    release = new CountDownLatch(1)
  }
}
