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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, ClusterBySpec}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, ResolveDefaultColumns}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, SupportsMetadataColumns, SupportsRead, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, IdentityTransform}
import org.apache.spark.sql.connector.read.SupportsReportStatistics
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

case class DescribeTableExec(
  output: Seq[Attribute],
  table: Table,
  isExtended: Boolean,
  asJson: Boolean = false) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    if (asJson) {
      runAsJson()
    } else {
      val rows = new ArrayBuffer[InternalRow]()
      addSchema(rows)
      addPartitioning(rows)
      addClustering(rows)

      if (isExtended) {
        addMetadataColumns(rows)
        addTableDetails(rows)
        addTableStats(rows)
      }

      rows.toSeq
    }
  }

  private def runAsJson(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    addSchemaJson(rows)
    addPartitioningJson(rows)
    addClustering(rows)

    if (isExtended) {
      addMetadataColumnsJson(rows)
      addTableDetailsJson(rows)
      addTableStatsJson(rows)
    }

    val jsonOutput = rows.map(row => row.getString(0)).mkString(",")
    val wrappedJson = s"""{"table_description": [$jsonOutput]}"""
    Seq(InternalRow(UTF8String.fromString(wrappedJson)))
  }

  private def addSchema(rows: ArrayBuffer[InternalRow]): Unit = {
    rows ++= table.schema.map { column =>
      toCatalystRow(column.name, column.dataType.simpleString, column.getComment().orNull)
    }
  }

  private def addSchemaJson(rows: ArrayBuffer[InternalRow]): Unit = {
    val schemaJson = table.schema.map { column =>
      s"""{
         |  "name": "${column.name}",
         |  "data_type": "${column.dataType.simpleString}",
         |  "comment": ${column.getComment().map(c => s""""$c"""").getOrElse("null")}
         |}""".stripMargin
    }.mkString("[", ",", "]")

    rows += InternalRow(UTF8String.fromString(s"""{"schema": $schemaJson}"""))
  }

  private def addMetadataColumns(rows: ArrayBuffer[InternalRow]): Unit = table match {
    case hasMeta: SupportsMetadataColumns if hasMeta.metadataColumns.nonEmpty =>
      rows += emptyRow()
      rows += toCatalystRow("# Metadata Columns", "", "")
      rows ++= hasMeta.metadataColumns.map { column =>
        toCatalystRow(
          column.name,
          column.dataType.simpleString,
          Option(column.comment()).getOrElse(""))
      }
    case _ =>
  }

  private def addMetadataColumnsJson(rows: ArrayBuffer[InternalRow]): Unit = {
    val metadataJson = table.schema.map { column =>
      s"""{
         |  "name": "${column.name}",
         |  "data_type": "${column.dataType.simpleString}",
         |  "comment": ${column.getComment().map(c => s""""$c"""").getOrElse("null")}
         |}""".stripMargin
    }.mkString("[", ",", "]")

    rows += InternalRow(UTF8String.fromString(s"""{"metadata_columns": $metadataJson}"""))
  }

  private def addPartitioning(rows: ArrayBuffer[InternalRow]): Unit = {
    // Clustering columns are handled in addClustering().
    val partitioning = table.partitioning
      .filter(t => !t.isInstanceOf[ClusterByTransform])
    if (partitioning.nonEmpty) {
      val partitionColumnsOnly = table.partitioning.forall(t => t.isInstanceOf[IdentityTransform])
      if (partitionColumnsOnly) {
        rows += toCatalystRow("# Partition Information", "", "")
        rows += toCatalystRow(s"# ${output(0).name}", output(1).name, output(2).name)
        rows ++= table.partitioning
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
          toCatalystRow(
            (path :+ field.name).map(quoteIfNeeded(_)).mkString("."),
            field.dataType.simpleString,
            field.getComment().orNull)
        }
      } else {
        rows += emptyRow()
        rows += toCatalystRow("# Partitioning", "", "")
        rows ++= table.partitioning.zipWithIndex.map {
          case (transform, index) => toCatalystRow(s"Part $index", transform.describe(), "")
        }
      }
    }
  }

  private def addPartitioningJson(rows: ArrayBuffer[InternalRow]): Unit = {
    val partitioning = table.partitioning.filter(!_.isInstanceOf[ClusterByTransform])

    val partitioningJson = if (partitioning.nonEmpty) {
      val partitionColumnsOnly = table.partitioning.forall(_.isInstanceOf[IdentityTransform])

      if (partitionColumnsOnly) {
        val partitionColumnsJson = table.partitioning.map { partition =>
          val fieldNames = partition.asInstanceOf[IdentityTransform].ref.fieldNames()
          val nestedField = table.schema.findNestedField(fieldNames.toImmutableArraySeq)

          nestedField match {
            case Some((path, field)) =>
              s"""{
                 |  "${output(0).name}": "${(path :+ field.name).map(quoteIfNeeded).mkString(".")}",
                 |  "${output(1).name}": "${field.dataType.simpleString}",
                 |  "${output(2).name}":
                 |  ${field.getComment().map(c => s""""$c"""").getOrElse("null")}
                 |}""".stripMargin
            case None =>
              throw QueryExecutionErrors.partitionColumnNotFoundInTheTableSchemaError(
                fieldNames.toSeq,
                table.schema()
              )
          }
        }.mkString("[", ",", "]")

        s"""{"partition_information": $partitionColumnsJson}""".stripMargin

      } else {
        val partitioningInfoJson = partitioning.zipWithIndex.map {
          case (transform, index) =>
            s"""{
               |  "part": "Part $index",
               |  "transform": "${transform.describe()}",
               |  "comment": null
               |}""".stripMargin
        }.mkString("[", ",", "]")

        s"""{"partitioning": $partitioningInfoJson}"""
      }
    } else {
      null
    }

    if (partitioningJson != null) {
      rows += InternalRow(UTF8String.fromString(partitioningJson))
    }
  }

  private def addClusteringToRows(
   clusterBySpec: ClusterBySpec,
   rows: ArrayBuffer[InternalRow]): Unit = {
    rows += toCatalystRow("# Clustering Information", "", "")
    rows += toCatalystRow(s"# ${output.head.name}", output(1).name, output(2).name)
    rows ++= clusterBySpec.columnNames.map { fieldNames =>
      val nestedField = table.schema.findNestedField(fieldNames.fieldNames.toIndexedSeq)
      assert(nestedField.isDefined,
        "The clustering column " +
          s"${fieldNames.fieldNames.map(quoteIfNeeded).mkString(".")} " +
          s"was not found in the table schema ${table.schema.catalogString}.")
      nestedField.get
    }.map { case (path, field) =>
      toCatalystRow(
        (path :+ field.name).map(quoteIfNeeded).mkString("."),
        field.dataType.simpleString,
        field.getComment().orNull)
    }
  }

  private def addClusteringToRowsJson(
     clusterBySpec: ClusterBySpec,
     rows: ArrayBuffer[InternalRow]
   ): Unit = {
    val clusteringJson = clusterBySpec.columnNames.map { fieldNames =>
      val nestedField = table.schema.findNestedField(fieldNames.fieldNames.toIndexedSeq)

      if (nestedField.isEmpty) {
        throw new IllegalArgumentException(
          "The clustering column " +
            s"${fieldNames.fieldNames.map(quoteIfNeeded).mkString(".")} " +
            s"was not found in the table schema ${table.schema.catalogString}."
        )
      }

      val (path, field) = nestedField.get
      s"""{
         |  "name": "${(path :+ field.name).map(quoteIfNeeded).mkString(".")}",
         |  "data_type": "${field.dataType.simpleString}",
         |  "comment": ${field.getComment().map(c => s""""$c"""").getOrElse("null")}
         |}""".stripMargin
    }.mkString("[", ",", "]")

    val clusteringInfoJson = s"""{"clustering_information": $clusteringJson}"""
    rows += InternalRow(UTF8String.fromString(clusteringInfoJson))
  }

  private def addClustering(rows: ArrayBuffer[InternalRow]): Unit = {
    ClusterBySpec.extractClusterBySpec(table.partitioning.toIndexedSeq).foreach { clusterBySpec =>
      if (!asJson) addClusteringToRows(clusterBySpec, rows)
      else addClusteringToRowsJson(clusterBySpec, rows)
    }
  }

  private def addTableStats(rows: ArrayBuffer[InternalRow]): Unit = table match {
    case read: SupportsRead =>
      read.newScanBuilder(CaseInsensitiveStringMap.empty()).build() match {
        case s: SupportsReportStatistics =>
          val stats = s.estimateStatistics()
          val statsComponents = Seq(
            Option.when(stats.sizeInBytes().isPresent)(s"${stats.sizeInBytes().getAsLong} bytes"),
            Option.when(stats.numRows().isPresent)(s"${stats.numRows().getAsLong} rows")
          ).flatten
          if (statsComponents.nonEmpty) {
            rows += toCatalystRow("Statistics", statsComponents.mkString(", "), null)
          }
        case _ =>
      }
    case _ =>
  }

  private def addTableStatsJson(rows: ArrayBuffer[InternalRow]): Unit = table match {
    case read: SupportsRead =>
      read.newScanBuilder(CaseInsensitiveStringMap.empty()).build() match {
        case s: SupportsReportStatistics =>
          val stats = s.estimateStatistics()
          val statsJsonComponents = Seq(
            stats.sizeInBytes().isPresent match {
              case true => s""""size_in_bytes": ${stats.sizeInBytes().getAsLong}"""
              case false => ""
            },
            stats.numRows().isPresent match {
              case true => s""""num_rows": ${stats.numRows().getAsLong}"""
              case false => ""
            }
          ).filter(_.nonEmpty).mkString(", ")

          if (statsJsonComponents.nonEmpty) {
            val statsJson = s"""{"table_statistics": {$statsJsonComponents}}"""
            rows += InternalRow(UTF8String.fromString(statsJson))
          }
        case _ =>
      }
    case _ =>
  }

  private def addTableDetails(rows: ArrayBuffer[InternalRow]): Unit = {
    rows += emptyRow()
    rows += toCatalystRow("# Detailed Table Information", "", "")
    rows += toCatalystRow("Name", table.name(), "")

    val tableType = if (table.properties().containsKey(TableCatalog.PROP_EXTERNAL)) {
      CatalogTableType.EXTERNAL.name
    } else {
      CatalogTableType.MANAGED.name
    }
    rows += toCatalystRow("Type", tableType, "")
    CatalogV2Util.TABLE_RESERVED_PROPERTIES
      .filterNot(_ == TableCatalog.PROP_EXTERNAL)
      .foreach(propKey => {
        if (table.properties.containsKey(propKey)) {
          rows += toCatalystRow(propKey.capitalize, table.properties.get(propKey), "")
        }
      })
    val properties =
      conf.redactOptions(table.properties.asScala.toMap).toList
        .filter(kv => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(kv._1))
        .sortBy(_._1).map {
        case (key, value) => key + "=" + value
      }.mkString("[", ",", "]")
    rows += toCatalystRow("Table Properties", properties, "")

    ResolveDefaultColumns.getDescribeMetadata(table.schema).foreach { row =>
      rows += toCatalystRow(row._1, row._2, row._3)
    }
  }

  private def addTableDetailsJson(rows: ArrayBuffer[InternalRow]): Unit = {
    val detailsType =
      if (table.properties.containsKey(TableCatalog.PROP_EXTERNAL)) "EXTERNAL" else "MANAGED"
    val detailsProperty =
      table.properties.asScala.map { case (key, value) => s""""$key": "$value"""" }.mkString(",")
    val detailsJson = s"""{
     |  "name": "${table.name()}",
     |  "type": "$detailsType",
     |  "properties": "$detailsProperty"
     |}""".stripMargin
    rows += InternalRow(UTF8String.fromString(s"""{"table_details": $detailsJson}"""))
  }

  private def emptyRow(): InternalRow = {
    InternalRow(UTF8String.fromString(""), UTF8String.fromString(""), UTF8String.fromString(""))
  }

  private def toCatalystRow(col1: String, col2: String, col3: String): InternalRow = {
    InternalRow(UTF8String.fromString(col1),
      UTF8String.fromString(col2),
      UTF8String.fromString(col3))
  }
}
