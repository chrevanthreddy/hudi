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

package org.apache.hudi

import org.apache.hudi.HoodieSparkUtils.{getCatalystRowSerDe, sparkAdapter}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.exception.SchemaCompatibilityException
import org.apache.hudi.internal.schema.HoodieSchemaException
import org.apache.hudi.io.storage.VectorConversionUtils

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType

import java.util.concurrent.ConcurrentHashMap

object AvroConversionUtils {
  private val ROW_TO_AVRO_CONVERTER_CACHE =
    new ConcurrentHashMap[Tuple3[StructType, Schema, Boolean], Function1[InternalRow, GenericRecord]]

  /**
   * Creates converter to transform Avro payload into Spark's Catalyst one
   *
   * @param rootAvroType Avro [[Schema]] to be transformed from
   * @param rootCatalystType Catalyst [[StructType]] to be transformed into
   * @return converter accepting Avro payload and transforming it into a Catalyst one (in the form of [[InternalRow]])
   */
  def createAvroToInternalRowConverter(rootAvroType: Schema, rootCatalystType: StructType): GenericRecord => Option[InternalRow] = {
    val deserializer = sparkAdapter.createAvroDeserializer(HoodieSchema.fromAvroSchema(rootAvroType), rootCatalystType)
    record => deserializer
      .deserialize(record)
      .map(_.asInstanceOf[InternalRow])
  }

  /**
   * Creates converter to transform Catalyst payload into Avro one
   *
   * @param rootCatalystType Catalyst [[StructType]] to be transformed from
   * @param rootAvroType Avro [[Schema]] to be transformed into
   * @param nullable whether Avro record is nullable
   * @return converter accepting Catalyst payload (in the form of [[InternalRow]]) and transforming it into an Avro one
   */
  def createInternalRowToAvroConverter(rootCatalystType: StructType, rootAvroType: Schema, nullable: Boolean): InternalRow => GenericRecord = {
    val loader: java.util.function.Function[Tuple3[StructType, Schema, Boolean], Function1[InternalRow, GenericRecord]] = key => {
      val hoodieSchema = HoodieSchema.fromAvroSchema(key._2)
      val vectorColumns = VectorConversionUtils.detectVectorColumns(hoodieSchema)
      val serializerInputSchema =
        if (vectorColumns.isEmpty) key._1 else VectorConversionUtils.replaceVectorColumnsWithBinary(key._1, vectorColumns)
      val serializer = sparkAdapter.createAvroSerializer(serializerInputSchema, hoodieSchema, key._3)
      val serializerInputRow =
        if (vectorColumns.isEmpty) {
          (row: InternalRow) => row
        } else {
          val mapper = VectorConversionUtils.buildBinaryRowMapper(
            key._1,
            vectorColumns,
            new java.util.function.Function[GenericInternalRow, InternalRow] {
              override def apply(row: GenericInternalRow): InternalRow = row
            })
          (row: InternalRow) => mapper.apply(row)
        }
      row => {
        try {
          serializer
            .serialize(serializerInputRow(row))
            .asInstanceOf[GenericRecord]
        } catch {
          case e: HoodieSchemaException => throw e
          case e => throw new SchemaCompatibilityException("Failed to convert spark record into avro record", e)
        }
      }
    }
    ROW_TO_AVRO_CONVERTER_CACHE.computeIfAbsent(Tuple3.apply(rootCatalystType, rootAvroType, nullable), loader)
  }

  /**
   * @deprecated please use [[AvroConversionUtils.createAvroToInternalRowConverter]]
   */
  @Deprecated
  def createConverterToRow(sourceAvroSchema: Schema,
                           targetSqlType: StructType): GenericRecord => Row = {
    val serde = getCatalystRowSerDe(targetSqlType)
    val converter = AvroConversionUtils.createAvroToInternalRowConverter(sourceAvroSchema, targetSqlType)

    avro => converter.apply(avro).map(serde.deserializeRow).get
  }

  /**
   * @deprecated please use [[AvroConversionUtils.createInternalRowToAvroConverter]]
   */
  @Deprecated
  def createConverterToAvro(sourceSqlType: StructType,
                            structName: String,
                            recordNamespace: String): Row => GenericRecord = {
    val serde = getCatalystRowSerDe(sourceSqlType)
    val schema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(sourceSqlType, structName, recordNamespace)
    val nullable = schema.isNullable

    val converter = AvroConversionUtils.createInternalRowToAvroConverter(sourceSqlType, schema.toAvroSchema, nullable)

    row => converter.apply(serde.serializeRow(row))
  }

  /**
   * Creates [[org.apache.spark.sql.DataFrame]] from the provided [[RDD]] of [[GenericRecord]]s
   *
   * TODO convert directly from GenericRecord into InternalRow instead
   */
  def createDataFrame(rdd: RDD[GenericRecord], schemaStr: String, ss: SparkSession): Dataset[Row] = {
    ss.createDataFrame(rdd.mapPartitions { records =>
      if (records.isEmpty) Iterator.empty
      else {
        val schema = HoodieSchema.parse(schemaStr)
        val dataType = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema)
        val converter = createConverterToRow(schema.toAvroSchema, dataType)
        records.map { r => converter(r) }
      }
    }, HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(HoodieSchema.parse(schemaStr)))
  }

  /**
   * Please use [[AvroSchemaUtils.getAvroRecordQualifiedName(String)]]
   */
  @Deprecated
  def getAvroRecordNameAndNamespace(tableName: String): (String, String) = {
    val sanitizedTableName = sanitizeAvroName(tableName)
    val qualifiedName = s"hoodie.$sanitizedTableName.${sanitizedTableName}_record"
    val nameParts = qualifiedName.split('.')
    (nameParts.last, nameParts.init.mkString("."))
  }

  private def sanitizeAvroName(name: String): String = {
    val invalidFirstCharRegex = "[^A-Za-z_]"
    val invalidCharRegex = "[^A-Za-z0-9_]"
    val firstPass =
      if (name.nonEmpty && name.substring(0, 1).matches(invalidFirstCharRegex)) {
        name.replaceFirst(invalidFirstCharRegex, "__")
      } else {
        name
      }
    firstPass.replaceAll(invalidCharRegex, "__")
  }
}
