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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, ClusterBySpec}
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericRowWithSchema}
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, ResolveDefaultColumns}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, SupportsMetadataColumns, SupportsRead, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, IdentityTransform}
import org.apache.spark.sql.connector.read.SupportsReportStatistics
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

object DescribeTableUtils extends V2CommandExec {

  def collectRows(table: Table, isExtended: Boolean): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    addSchema(rows, table)
    addPartitioning(rows, table)
    addClustering(rows, table)

    if (isExtended) {
      addMetadataColumns(rows, table)
      addTableDetails(rows, table)
      addTableStats(rows, table)
    }
    rows.toSeq
  }

  private def addTableDetails(rows: ArrayBuffer[InternalRow], table: Table): Unit = {
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

    // If any columns have default values, append them to the result.
    ResolveDefaultColumns.getDescribeMetadata(table.schema).foreach { row =>
      rows += toCatalystRow(row._1, row._2, row._3)
    }
  }

  private def addSchema(rows: ArrayBuffer[InternalRow], table: Table): Unit = {
    rows ++= table.schema.map { column =>
      toCatalystRow(
        column.name, column.dataType.simpleString, column.getComment().orNull)
    }
  }

  private def addMetadataColumns(rows: ArrayBuffer[InternalRow], table: Table): Unit = table match {
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

  private def addPartitioning(rows: ArrayBuffer[InternalRow], table: Table): Unit = {
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

  private def addClusteringToRows(
   clusterBySpec: ClusterBySpec,
   rows: ArrayBuffer[InternalRow],
   table: Table): Unit = {
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

  private def addClustering(rows: ArrayBuffer[InternalRow], table: Table): Unit = {
    ClusterBySpec.extractClusterBySpec(table.partitioning.toIndexedSeq).foreach { clusterBySpec =>
      addClusteringToRows(clusterBySpec, rows, table)
    }
  }

  private def addTableStats(rows: ArrayBuffer[InternalRow], table: Table): Unit = table match {
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

  private def emptyRow(): InternalRow = toCatalystRow("", "", "")

  override protected def run(): Seq[InternalRow] = Seq.empty

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[SparkPlan] = Seq.empty

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    this

  override def productArity: Int = 0

  override def productElement(n: Int): Any = null

  override def canEqual(that: Any): Boolean = false
}

case class DescribeTableExec(
    output: Seq[Attribute],
    table: Table,
    isExtended: Boolean) extends LeafV2CommandExec {
  override protected def run(): Seq[InternalRow] = {
    DescribeTableUtils.collectRows(table, isExtended)
  }
}

case class DescribeTableAsJsonExec(
  output: Seq[Attribute],
  table: Table,
  isExtended: Boolean) extends LeafV2CommandExec {

  override def run(): Seq[InternalRow] = {
    // Execute the original DescribeTableExec command to get the rows
    val rows: Seq[InternalRow] = DescribeTableUtils.collectRows(table, isExtended)

    val jsonArray = rows.map { row =>
      val schema = table.schema
      val rowAsRow = internalRowToRow(row, schema)
      rowToJson(rowAsRow)
    }.mkString("[", ",", "]")

    Seq(InternalRow(UTF8String.fromString(jsonArray)))
  }

  private def internalRowToRow(internalRow: InternalRow, schema: StructType): Row = {
    new GenericRowWithSchema(internalRow.toSeq(schema).toArray, schema)
  }

  private def rowToJson(row: Row): String = {
    row.json
  }
}
