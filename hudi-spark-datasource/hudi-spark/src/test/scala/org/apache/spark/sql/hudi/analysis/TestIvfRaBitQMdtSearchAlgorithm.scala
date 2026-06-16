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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.index.vector.VectorIndexOptions
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.index.HoodieSparkIndexClient
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.{Row, SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals

import java.util.function.Consumer

import scala.collection.JavaConverters._

class TestIvfRaBitQMdtSearchAlgorithm extends HoodieSparkClientTestBase {

  private var spark: SparkSession = _
  private var tablePath: String = _

  override def getSparkSessionExtensionsInjector: HOption[Consumer[SparkSessionExtensions]] =
    HOption.of(
      JFunction.toJavaConsumer((receiver: SparkSessionExtensions) =>
        new HoodieSparkSessionExtension().apply(receiver)))

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
    IvfRaBitQMdtSearchAlgorithm.resetMetadataCaches()
    tablePath = s"$basePath/vector_ivf_mdt"
    writeCorpusTable()
    createVectorIndex(dim = 4)
  }

  @AfterEach override def tearDown(): Unit = {
    IvfRaBitQMdtSearchAlgorithm.resetMetadataCaches()
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @Test
  def testIvfRaBitQMdtSearchUsesDriverMetadataCache(): Unit = {
    val expectedIds = Array("doc_1", "doc_4")

    val firstRun = runQuery()
    assertEquals(expectedIds.toSeq, firstRun.map(_.getAs[String]("id")).toSeq)
    assertEquals(1, IvfRaBitQMdtSearchAlgorithm.metadataCacheSize)

    val secondRun = runQuery()
    assertEquals(expectedIds.toSeq, secondRun.map(_.getAs[String]("id")).toSeq)
    assertEquals(1, IvfRaBitQMdtSearchAlgorithm.metadataCacheSize)
  }

  private def runQuery(): Array[org.apache.spark.sql.Row] = {
    spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$tablePath',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0, 0.0),
         |  2,
         |  'l2',
         |  'ivf_rabitq_mdt'
         |)
         |ORDER BY _hudi_distance, id
         |""".stripMargin
    ).collect()
  }

  private def writeCorpusTable(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(4)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("ts", LongType, nullable = false),
      StructField("partition_path", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, metadata),
      StructField("label", StringType, nullable = true)
    ))

    val rows = Seq(
      Row("doc_1", 1L, "p0", Seq(1.0f, 0.0f, 0.0f, 0.0f), "x-axis"),
      Row("doc_2", 2L, "p0", Seq(0.0f, 1.0f, 0.0f, 0.0f), "y-axis"),
      Row("doc_3", 3L, "p1", Seq(0.0f, 0.0f, 1.0f, 0.0f), "z-axis"),
      Row("doc_4", 4L, "p1", Seq(0.9f, 0.1f, 0.0f, 0.0f), "near-x"),
      Row("doc_5", 5L, "p0", Seq(0.0f, 0.0f, 0.0f, 1.0f), "w-axis")
    )

    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PARTITIONPATH_FIELD.key, "partition_path")
      .option(PRECOMBINE_FIELD.key, "ts")
      .option(TABLE_NAME.key, "vector_ivf_mdt_table")
      .option(TABLE_TYPE.key, "COPY_ON_WRITE")
      .option("hoodie.metadata.enable", "true")
      .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.InProcessLockProvider")
      .mode(SaveMode.Overwrite)
      .save(tablePath)
  }

  private def createVectorIndex(dim: Int): Unit = {
    val options = Map(
      "vector.dimension" -> dim.toString,
      "vector.num_clusters" -> "2",
      "vector.metric" -> "l2",
      "vector.max_iter" -> "5",
      "vector.quantizer" -> "IVF_RABITQ",
      VectorIndexOptions.RABITQ_MATERIALIZE_ON_CREATE -> "false"
    )

    val metaClient = createMetaClient(spark, tablePath)
    new HoodieSparkIndexClient(spark).create(
      metaClient,
      "embedding_idx",
      "vector_index",
      Map("embedding" -> Map.empty[String, String].asJava).asJava,
      options.asJava,
      Map.empty[String, String].asJava)
  }
}
