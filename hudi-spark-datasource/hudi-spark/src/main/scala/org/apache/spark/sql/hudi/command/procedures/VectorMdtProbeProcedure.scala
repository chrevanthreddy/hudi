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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.index.vector.VectorIndexMdtSearchUtils
import org.apache.hudi.common.index.vector.VectorIndexMdtSearchUtils.{PostingMatch, ScoredPostingMatch}
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.metadata.HoodieBackedTableMetadata
import org.apache.hudi.storage.HoodieStorageUtils

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier

import scala.collection.JavaConverters._

class VectorMdtProbeProcedure extends BaseProcedure with ProcedureBuilder with Logging {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "path", DataTypes.StringType),
    ProcedureParameter.required(1, "index_partition", DataTypes.StringType),
    ProcedureParameter.required(2, "generation_id", DataTypes.IntegerType),
    ProcedureParameter.required(3, "cluster_shard_counts", DataTypes.StringType),
    ProcedureParameter.required(4, "query_key", DataTypes.StringType),
    ProcedureParameter.optional(5, "embedding_col", DataTypes.StringType, "embedding"),
    ProcedureParameter.optional(6, "dimension", DataTypes.IntegerType, 1536),
    ProcedureParameter.optional(7, "seed", DataTypes.LongType, 42L),
    ProcedureParameter.optional(8, "assume_normalized", DataTypes.BooleanType, false),
    ProcedureParameter.optional(9, "k", DataTypes.IntegerType, 10),
    ProcedureParameter.optional(10, "refine_factor", DataTypes.IntegerType, 5)
  )

  private val OUTPUT_TYPE = new StructType(Array(
    StructField("metric", DataTypes.StringType, nullable = false, Metadata.empty),
    StructField("value", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    checkArgs(PARAMETERS, args)

    val basePath = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val indexPartition = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val generationId = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]
    val clusterShardCountsRaw = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String]
    val queryKey = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]
    val embeddingCol = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[String]
    val dimension = getArgValueOrDefault(args, PARAMETERS(6)).get.asInstanceOf[Int]
    val seed = asLong(getArgValueOrDefault(args, PARAMETERS(7)).get)
    val assumeNormalized = getArgValueOrDefault(args, PARAMETERS(8)).get.asInstanceOf[Boolean]
    val k = getArgValueOrDefault(args, PARAMETERS(9)).get.asInstanceOf[Int]
    val refineFactor = getArgValueOrDefault(args, PARAMETERS(10)).get.asInstanceOf[Int]
    val refineK = Math.max(k, k * refineFactor)

    val shardCounts = parseClusterShardCounts(clusterShardCountsRaw)
    val rows = new util.ArrayList[Row]()
    def add(metric: String, value: Any): Unit = rows.add(Row(metric, String.valueOf(value)))

    add("basePath", basePath)
    add("indexPartition", indexPartition)
    add("generationId", generationId)
    add("clusterShardCounts", clusterShardCountsRaw)
    add("postingPrefixCount", VectorIndexMdtSearchUtils.buildPostingPrefixes(generationId, shardCounts).size())

    val storage = HoodieStorageUtils.getStorage(basePath, HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()))
    val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build()
    val metadataTable = new HoodieBackedTableMetadata(new HoodieSparkEngineContext(jsc), storage, metadataConfig, basePath)
    try {
      val readStartNs = System.nanoTime()
      val postings: HoodieData[PostingMatch] =
        VectorIndexMdtSearchUtils.readPostingMatches(metadataTable, indexPartition, generationId, shardCounts, false)
      val postingsRdd = HoodieJavaRDD.getJavaRDD(postings).rdd
      val perPartition = postingsRdd.mapPartitionsWithIndex { case (idx, iter) =>
        val task = Option(TaskContext.get())
          .map(ctx => s"stage=${ctx.stageId()} partition=${ctx.partitionId()} attempt=${ctx.attemptNumber()}")
          .getOrElse("stage=unknown partition=unknown attempt=unknown")
        var count = 0L
        val fileGroups = scala.collection.mutable.Set.empty[String]
        while (iter.hasNext) {
          val posting = iter.next()
          count += 1
          if (posting.getFileGroupId != null) {
            fileGroups += posting.getFileGroupId
          }
        }
        Iterator((idx, count, fileGroups.size, task))
      }.collect().sortBy(_._1)
      val readMs = elapsedMs(readStartNs)
      val postingCount = perPartition.map(_._2).sum
      add("directPostingReadMs", readMs)
      add("directPostingCount", postingCount)
      add("directPostingPartitions", perPartition.length)
      perPartition.foreach { case (idx, count, fgCount, task) =>
        add(s"postingPartition.$idx", s"count=$count distinctBaseFileGroups=$fgCount $task")
      }

      val queryVector = loadQueryVector(SparkSession.active, basePath, queryKey, embeddingCol)
      val scoreStartNs = System.nanoTime()
      val postingsForScore: HoodieData[PostingMatch] =
        VectorIndexMdtSearchUtils.readPostingMatches(metadataTable, indexPartition, generationId, shardCounts, false)
      val scored: HoodieData[ScoredPostingMatch] =
        VectorIndexMdtSearchUtils.scorePostingMatches(postingsForScore, queryVector, dimension, seed, assumeNormalized)
      val topCandidates = VectorIndexMdtSearchUtils.selectTopK(scored, refineK)
      val topList = topCandidates.collectAsList().asScala.toSeq.sortBy(c => (c.getApproxDistance, c.getRecordKey))
      val scoreMs = elapsedMs(scoreStartNs)
      add("scoreAndReduceMs", scoreMs)
      add("refineK", refineK)
      add("topCandidateCount", topList.size)
      add("refineDistinctBaseFileGroups", topList.flatMap(c => Option(c.getFileGroupId)).distinct.size)
      add("topKDistinctBaseFileGroups", topList.take(k).flatMap(c => Option(c.getFileGroupId)).distinct.size)
      topList.take(k).zipWithIndex.foreach { case (candidate, i) =>
        add(s"topK.${i + 1}",
          s"dist=${candidate.getApproxDistance} record=${candidate.getRecordKey} fg=${candidate.getFileGroupId}")
      }
    } finally {
      metadataTable.close()
    }

    rows.asScala.toSeq
  }

  override def build: Procedure = new VectorMdtProbeProcedure

  private def parseClusterShardCounts(raw: String): util.Map[Integer, Integer] = {
    val result = new util.HashMap[Integer, Integer]()
    raw.split(",").map(_.trim).filter(_.nonEmpty).foreach { token =>
      val parts = token.split(":")
      if (parts.length != 2) {
        throw new IllegalArgumentException(s"Invalid cluster_shard_counts token '$token'. Expected cluster:shards")
      }
      result.put(Integer.valueOf(parts(0).trim.toInt), Integer.valueOf(Math.max(1, parts(1).trim.toInt)))
    }
    result
  }

  private def loadQueryVector(spark: SparkSession, basePath: String, queryKey: String, embeddingCol: String): Array[Float] = {
    val row = spark.read.format("hudi").load(basePath)
      .where(col("_hoodie_record_key") === queryKey)
      .select(embeddingCol)
      .limit(1)
      .collect()
      .headOption
      .getOrElse(throw new IllegalArgumentException(s"Query key not found: $queryKey"))
    row.getAs[Seq[Float]](0).toArray
  }

  private def asLong(value: Any): Long = value match {
    case v: Long => v
    case v: Int => v.toLong
    case v: Short => v.toLong
    case v: Byte => v.toLong
    case other => other.toString.toLong
  }

  private def elapsedMs(startNs: Long): Long = (System.nanoTime() - startNs) / 1000000L
}

object VectorMdtProbeProcedure {
  val NAME = "vector_mdt_probe"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new VectorMdtProbeProcedure
  }
}
