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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.json4s._
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, ClusterBySpec}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.DescribeCommandSchema
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, SupportsMetadataColumns, SupportsRead, TableCatalog}
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, IdentityTransform}
import org.apache.spark.sql.connector.read.SupportsReportStatistics
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

case class DescribeTableJsonExec(
    resolvedTable: ResolvedTable,
    isExtended: Boolean) extends LeafV2CommandExec {
  override def output: Seq[Attribute] = DescribeCommandSchema.describeTableAttributes()

  private val table = resolvedTable.table
  override protected def run(): Seq[InternalRow] = {
    val jsonMap = mutable.LinkedHashMap[String, JValue]()
    describeColsJson(table.schema, jsonMap)
    addPartitioning(jsonMap)
    addMetadataColumns(jsonMap)
    addClustering(jsonMap)
    addTableDetails(jsonMap)
    addTableStats(jsonMap)

    val utf8JsonString: UTF8String = UTF8String.fromString(compact(render(JObject(jsonMap.toList))))

    Seq(InternalRow(utf8JsonString))
  }

  private def addTableDetails(jsonMap: mutable.LinkedHashMap[String, JValue]): Unit = {
    addKeyValueToMap("catalog_name", JString(resolvedTable.catalog.name()), jsonMap)
    addKeyValueToMap("namespace",
      JArray(resolvedTable.identifier.namespace().map(JString).toList),
      jsonMap)
    addKeyValueToMap("schema_name", JString(resolvedTable.identifier.namespace().last), jsonMap)
    addKeyValueToMap("table_name", JString(resolvedTable.identifier.name()), jsonMap)

    val tableType = if (table.properties().containsKey(TableCatalog.PROP_EXTERNAL)) {
      CatalogTableType.EXTERNAL.name
    } else {
      CatalogTableType.MANAGED.name
    }
    addKeyValueToMap("type", JString(tableType), jsonMap)
    CatalogV2Util.TABLE_RESERVED_PROPERTIES
      .filterNot(_ == TableCatalog.PROP_EXTERNAL)
      .foreach(propKey => {
        if (table.properties.containsKey(propKey)) {
          addKeyValueToMap(propKey, JString(table.properties.get(propKey)), jsonMap)
        }
      })
    val properties = JObject(
      conf.redactOptions(table.properties.asScala.toMap).toList
        .filter(kv => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(kv._1))
        .sortBy(_._1)
        .map { case (key, value) => key -> JString(value) }
    )
    addKeyValueToMap("table_properties", properties, jsonMap)
  }

  private def addKeyValueToMap(
    key: String,
    value: JValue,
    jsonMap: mutable.LinkedHashMap[String, JValue]): Unit = {
    // Rename some JSON keys that are pre-named in describe table implementation
    val renames = Map(
      "inputformat" -> "input_format",
      "outputformat" -> "output_format"
    )

    val normalizedKey = key.toLowerCase().replace(" ", "_")
    val renamedKey = renames.getOrElse(normalizedKey, normalizedKey)

    if (!jsonMap.contains(renamedKey)) {
      jsonMap += renamedKey -> value
    }
  }

  /**
   * Util to recursively form JSON string representation of data type, used for DESCRIBE AS JSON.
   * Differs from `json` in DataType.scala by providing additional fields for some types.
   */
  private def jsonType(
    dataType: DataType): JValue = {
    dataType match {
      case arrayType: ArrayType =>
        JObject(
          "name" -> JString("array"),
          "element_type" -> jsonType(arrayType.elementType),
          "element_nullable" -> JBool(arrayType.containsNull)
        )

      case mapType: MapType =>
        JObject(
          "name" -> JString("map"),
          "key_type" -> jsonType(mapType.keyType),
          "value_type" -> jsonType(mapType.valueType),
          "value_nullable" -> JBool(mapType.valueContainsNull)
        )

      case structType: StructType =>
        val fieldsJson = structType.fields.map { field =>
          val baseJson = List(
            "name" -> JString(field.name),
            "type" -> jsonType(field.dataType),
            "nullable" -> JBool(field.nullable)
          )
          val commentJson = field.getComment().map(comment => "comment" -> JString(comment)).toList
          val defaultJson =
            field.getCurrentDefaultValue().map(default => "default" -> JString(default)).toList

          JObject(baseJson ++ commentJson ++ defaultJson: _*)
        }.toList

        JObject(
          "name" -> JString("struct"),
          "fields" -> JArray(fieldsJson)
        )

      case decimalType: DecimalType =>
        JObject(
          "name" -> JString("decimal"),
          "precision" -> JInt(decimalType.precision),
          "scale" -> JInt(decimalType.scale)
        )

      case varcharType: VarcharType =>
        JObject(
          "name" -> JString("varchar"),
          "length" -> JInt(varcharType.length)
        )

      case charType: CharType =>
        JObject(
          "name" -> JString("char"),
          "length" -> JInt(charType.length)
        )

      // Only override TimestampType; TimestampType_NTZ type is already timestamp_ntz
      case _: TimestampType =>
        JObject("name" -> JString("timestamp_ltz"))

      case yearMonthIntervalType: YearMonthIntervalType =>
        def getFieldName(field: Byte): String = YearMonthIntervalType.fieldToString(field)

        JObject(
          "name" -> JString("interval"),
          "start_unit" -> JString(getFieldName(yearMonthIntervalType.startField)),
          "end_unit" -> JString(getFieldName(yearMonthIntervalType.endField))
        )

      case dayTimeIntervalType: DayTimeIntervalType =>
        def getFieldName(field: Byte): String = DayTimeIntervalType.fieldToString(field)

        JObject(
          "name" -> JString("interval"),
          "start_unit" -> JString(getFieldName(dayTimeIntervalType.startField)),
          "end_unit" -> JString(getFieldName(dayTimeIntervalType.endField))
        )

      case _ =>
        JObject("name" -> JString(dataType.typeName))
    }
  }

  private def describeColsJson(
    schema: StructType,
    jsonMap: mutable.LinkedHashMap[String, JValue],
    keyName: String = "columns"): Unit = {
    val columnsJson = jsonType(StructType(schema.fields))
      .asInstanceOf[JObject].find(_.isInstanceOf[JArray]).get
    addKeyValueToMap(keyName, columnsJson, jsonMap)
  }

  private def addMetadataColumns(jsonMap: mutable.LinkedHashMap[String, JValue]):
      Unit = table match {
    case hasMeta: SupportsMetadataColumns if hasMeta.metadataColumns.nonEmpty =>
      val metadataColumns = JArray(
        hasMeta.metadataColumns.map { column =>
          JObject(List(
            "name" -> JString(column.name),
            "type" -> jsonType(column.dataType),
            "comment" -> JString(Option(column.comment()).getOrElse(""))
          ))
        }.toList
      )
    addKeyValueToMap("metadata_columns", metadataColumns, jsonMap)
    case _ =>
  }

  private def addClusteringToRows(
       clusterBySpec: ClusterBySpec,
       jsonMap: mutable.LinkedHashMap[String, JValue]): Unit = {
    val value = clusterBySpec.columnNames.map { fieldNames =>
      val nestedField = table.schema.findNestedField(fieldNames.fieldNames.toIndexedSeq)
      assert(nestedField.isDefined,
        "The clustering column " +
          s"${fieldNames.fieldNames.map(quoteIfNeeded).mkString(".")} " +
          s"was not found in the table schema ${table.schema.catalogString}.")
      nestedField.get
    }.map { case (path, field) =>
      (path :+ field.name).map(quoteIfNeeded(_)).mkString(".")
    }.toString
    addKeyValueToMap("clustering_columns", JString(value), jsonMap)
  }

  private def addClustering(jsonMap: mutable.LinkedHashMap[String, JValue]): Unit = {
    ClusterBySpec.extractClusterBySpec(table.partitioning.toIndexedSeq).foreach { clusterBySpec =>
      addClusteringToRows(clusterBySpec, jsonMap)
    }
  }

  private def addTableStats(jsonMap: mutable.LinkedHashMap[String, JValue]): Unit = table match {
    case read: SupportsRead =>
      read.newScanBuilder(CaseInsensitiveStringMap.empty()).build() match {
        case s: SupportsReportStatistics =>
          val stats = s.estimateStatistics()
          val statsComponents = Seq(
            Option.when(stats.sizeInBytes().isPresent)(s"${stats.sizeInBytes().getAsLong} bytes"),
            Option.when(stats.numRows().isPresent)(s"${stats.numRows().getAsLong} rows")
          ).flatten
          if (statsComponents.nonEmpty) {
            addKeyValueToMap("statistics", JArray(statsComponents.map(JString).toList), jsonMap)
          }
        case _ =>
      }
    case _ =>
  }

  private def addPartitioning(jsonMap: mutable.LinkedHashMap[String, JValue]): Unit = {
    // Clustering columns are handled in addClustering().
    val partitioning = table.partitioning
      .filter(t => !t.isInstanceOf[ClusterByTransform])
    if (partitioning.nonEmpty) {
      val partitionColumnsOnly = table.partitioning.forall(t => t.isInstanceOf[IdentityTransform])
      if (partitionColumnsOnly) {
        val value = table.partitioning
          .map(_.asInstanceOf[IdentityTransform].ref.fieldNames())
          .map { fieldNames =>
            val nestedField = table.schema.findNestedField(fieldNames.toImmutableArraySeq)
            if (nestedField.isEmpty) {
              throw QueryExecutionErrors.partitionColumnNotFoundInTheTableSchemaError(
                fieldNames.toSeq,
                table.schema()
              )
            }
            nestedField.get
          }.map { case (path, field) =>
            (path :+ field.name).map(quoteIfNeeded(_)).mkString(".")
          }.toString
        addKeyValueToMap("partition_columns", JString(value), jsonMap)
      } else {
        val partitionArr = JArray(
          table.partitioning.map {
            transform => JString(transform.describe())
          }.toList
        )
        addKeyValueToMap("partition_columns", partitionArr, jsonMap)
      }
    }
  }
}
