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

package org.apache.spark.sql.execution.command.v2

import java.util.Locale

import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Utils


/**
 * The class contains tests for the `DESCRIBE TABLE` command to check V2 table catalogs.
 */
class DescribeTableSuite extends command.DescribeTableSuiteBase
  with CommandSuiteBase {
  implicit val formats: org.json4s.DefaultFormats.type = org.json4s.DefaultFormats

  def getProvider(): String = defaultUsing.stripPrefix("USING").trim.toLowerCase(Locale.ROOT)


  test("Describing a partition is not supported") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing " +
        "PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"DESCRIBE TABLE $tbl PARTITION (id = 1)")
      }
      assert(e.message === "DESCRIBE does not support partition for v2 tables.")
    }
  }

  test("DESCRIBE TABLE of a partitioned table by nested columns") {
    withNamespaceAndTable("ns", "table") { tbl =>
      sql(s"CREATE TABLE $tbl (s struct<id:INT, a:BIGINT>, data string) " +
        s"$defaultUsing PARTITIONED BY (s.id, s.a)")
      val descriptionDf = sql(s"DESCRIBE TABLE $tbl")
      QueryTest.checkAnswer(
        descriptionDf.filter("col_name != 'Created Time'"),
        Seq(
          Row("data", "string", null),
          Row("s", "struct<id:int,a:bigint>", null),
          Row("# Partition Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("s.id", "int", null),
          Row("s.a", "bigint", null)))
    }
  }

  test("DESCRIBE TABLE EXTENDED of a partitioned table") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing" +
        " PARTITIONED BY (id)" +
        " TBLPROPERTIES ('bar'='baz')" +
        " COMMENT 'this is a test table'" +
        " LOCATION 'file:/tmp/testcat/table_name'")
      val descriptionDf = spark.sql(s"DESCRIBE TABLE EXTENDED $tbl")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      QueryTest.checkAnswer(
        descriptionDf,
        Seq(
          Row("id", "bigint", null),
          Row("data", "string", null),
          Row("# Partition Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("id", "bigint", null),
          Row("", "", ""),
          Row("# Metadata Columns", "", ""),
          Row("index", "int", "Metadata column used to conflict with a data column"),
          Row("_partition", "string", "Partition key used to store the row"),
          Row("", "", ""),
          Row("# Detailed Table Information", "", ""),
          Row("Name", tbl, ""),
          Row("Type", "MANAGED", ""),
          Row("Comment", "this is a test table", ""),
          Row("Location", "file:/tmp/testcat/table_name", ""),
          Row("Provider", "_", ""),
          Row(TableCatalog.PROP_OWNER.capitalize, Utils.getCurrentUserName(), ""),
          Row("Table Properties", "[bar=baz]", ""),
          Row("Statistics", "0 bytes, 0 rows", null)))
    }
  }

  test("describe a non-existent column") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"""
        |CREATE TABLE $tbl
        |(key int COMMENT 'column_comment', col struct<x:int, y:string>)
        |$defaultUsing""".stripMargin)
      val query = s"DESC $tbl key1"
      checkError(
        exception = intercept[AnalysisException] {
          sql(query).collect()
        },
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map(
          "objectName" -> "`key1`",
          "proposal" -> "`key`, `col`"),
        context = ExpectedContext(
          fragment = query,
          start = 0,
          stop = query.length -1)
      )
    }
  }

  test("describe a column in case insensitivity") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withNamespaceAndTable("ns", "tbl") { tbl =>
        sql(s"CREATE TABLE $tbl (key int COMMENT 'comment1') $defaultUsing")
        QueryTest.checkAnswer(
          sql(s"DESC $tbl KEY"),
          Seq(Row("col_name", "KEY"), Row("data_type", "int"), Row("comment", "comment1")))
      }
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withNamespaceAndTable("ns", "tbl") { tbl =>
        sql(s"CREATE TABLE $tbl (key int COMMENT 'comment1') $defaultUsing")
        val query = s"DESC $tbl KEY"
        checkError(
          exception = intercept[AnalysisException] {
            sql(query).collect()
          },
          condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
          sqlState = "42703",
          parameters = Map(
            "objectName" -> "`KEY`",
            "proposal" -> "`key`"),
          context = ExpectedContext(
            fragment = query,
            start = 0,
            stop = query.length - 1))
      }
    }
  }

  test("describe extended (formatted) a column") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(s"""
        |CREATE TABLE $tbl
        |(key INT COMMENT 'column_comment', col STRING)
        |$defaultUsing""".stripMargin)

      sql(s"INSERT INTO $tbl values (1, 'aaa'), (2, 'bbb'), (3, 'ccc'), (null, 'ddd')")
      val descriptionDf = sql(s"DESCRIBE TABLE EXTENDED $tbl key")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("info_name", StringType),
        ("info_value", StringType)))
      QueryTest.checkAnswer(
        descriptionDf,
        Seq(
          Row("col_name", "key"),
          Row("data_type", "int"),
          Row("comment", "column_comment"),
          Row("min", "NULL"),
          Row("max", "NULL"),
          Row("num_nulls", "1"),
          Row("distinct_count", "4"),
          Row("avg_col_len", "NULL"),
          Row("max_col_len", "NULL")))
    }
  }

  test("SPARK-46535: describe extended (formatted) a column without col stats") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(
        s"""
           |CREATE TABLE $tbl
           |(key INT COMMENT 'column_comment', col STRING)
           |$defaultUsing""".stripMargin)

      val descriptionDf = sql(s"DESCRIBE TABLE EXTENDED $tbl key")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("info_name", StringType),
        ("info_value", StringType)))
      QueryTest.checkAnswer(
        descriptionDf,
        Seq(
          Row("col_name", "key"),
          Row("data_type", "int"),
          Row("comment", "column_comment")))
    }
  }

  test("describe extended table with stats") {
    withNamespaceAndTable("ns", "tbl") { tbl =>
      sql(
        s"""
           |CREATE TABLE $tbl
           |(key INT, col STRING)
           |$defaultUsing""".stripMargin)

      sql(s"INSERT INTO $tbl values (1, 'aaa'), (2, 'bbb'), (3, 'ccc'), (null, 'ddd')")
      val descriptionDf = sql(s"DESCRIBE TABLE EXTENDED $tbl")
      val stats = descriptionDf.filter("col_name == 'Statistics'").head()
        .getAs[String]("data_type")
      assert("""\d+\s+bytes,\s+4\s+rows""".r.matches(stats))
    }
  }

  test("DESCRIBE AS JSON throws when not EXTENDED") {
    withNamespaceAndTable("ns", "table") { t =>
      val tableCreationStr =
        s"""
           |CREATE TABLE $t (
           |  employee_id INT,
           |  employee_name STRING,
           |  department STRING,
           |  hire_date DATE
           |) USING parquet
           |OPTIONS ('compression' = 'snappy', 'max_records' = '1000')
           |PARTITIONED BY (department, hire_date)
           |CLUSTERED BY (employee_id) SORTED BY (employee_name ASC) INTO 4 BUCKETS
           |COMMENT 'Employee data table for testing partitions and buckets'
           |TBLPROPERTIES ('version' = '1.0')
           |""".stripMargin
      spark.sql(tableCreationStr)

      val error = intercept[AnalysisException] {
        spark.sql(s"DESCRIBE $t AS JSON")
      }

      checkError(
        exception = error,
        condition = "DESCRIBE_JSON_NOT_EXTENDED",
        parameters = Map("tableName" -> "table"))
    }
  }

  test("DESCRIBE AS JSON partitions, clusters, buckets") {
    withNamespaceAndTable("ns", "table") { t =>
      val tableCreationStr =
        s"""
           |CREATE TABLE $t (
           |  employee_id INT,
           |  employee_name STRING,
           |  department STRING,
           |  hire_date DATE
           |) USING parquet
           |OPTIONS ('compression' = 'snappy', 'max_records' = '1000')
           |PARTITIONED BY (department, hire_date)
           |CLUSTERED BY (employee_id) SORTED BY (employee_name ASC) INTO 4 BUCKETS
           |COMMENT 'Employee data table for testing partitions and buckets'
           |TBLPROPERTIES ('version' = '1.0')
           |""".stripMargin
      spark.sql(tableCreationStr)
      val descriptionDf = spark.sql(s"DESCRIBE EXTENDED $t AS JSON")
      val firstRow = descriptionDf.select("json_metadata").head()
      val jsonValue = firstRow.getString(0)
      val parsedOutput = parse(jsonValue).extract[DescribeTableJson]

      val expectedOutput = DescribeTableJson(
        catalog_name = Some("test_catalog"),
        namespace = Some(List("ns")),
        schema_name = Some("ns"),
        table_name = Some("table"),
        columns = Some(List(
          TableColumn("employee_id", Type("integer"), true),
          TableColumn("employee_name", Type("string"), true),
          TableColumn("department", Type("string"), true),
          TableColumn("hire_date", Type("date"), true)
        )),
        partition_columns = Some(List(
          "department",
          "hire_date",
          "sorted_bucket(employee_id, 4, employee_name)"
        )),
        metadata_columns = Some(List(
          TableColumn(
            name = "index",
            `type` = Type("integer"),
            comment = Some("Metadata column used to conflict with a data column")
          ),
          TableColumn(
            name = "_partition",
            `type` = Type("string"),
            comment = Some("Partition key used to store the row")
          )
        )),
        `type` = Some("MANAGED"),
        comment = Some("Employee data table for testing partitions and buckets"),
        provider = Some("parquet"),
        table_properties = Some(Map(
          "compression" -> "snappy",
          "max_records" -> "1000",
          "option.compression" -> "snappy",
          "option.max_records" -> "1000",
          "version" -> "1.0"
        )),
        statistics = Some(List("0 bytes", "0 rows"))
      )

      if (getProvider() == "hive") {
        assert(expectedOutput == parsedOutput.copy(owner = None))
      } else {
        assert(expectedOutput.copy(inputformat = None, outputformat = None, serde_library = None)
          == parsedOutput.copy(owner = None))
      }
    }
  }
}

/** Represents JSON output of DESCRIBE TABLE AS JSON */
case class DescribeTableJson(
    table_name: Option[String] = None,
    catalog_name: Option[String] = None,
    namespace: Option[List[String]] = Some(Nil),
    schema_name: Option[String] = None,
    columns: Option[List[TableColumn]] = Some(Nil),
    owner: Option[String] = None,
    created_time: Option[String] = None,
    last_access: Option[String] = None,
    created_by: Option[String] = None,
    `type`: Option[String] = None,
    provider: Option[String] = None,
    bucket_columns: Option[List[String]] = Some(Nil),
    sort_columns: Option[List[String]] = Some(Nil),
    comment: Option[String] = None,
    table_properties: Option[Map[String, String]] = None,
    location: Option[String] = None,
    metadata_columns: Option[List[TableColumn]] = Some(Nil),
    statistics: Option[List[String]] = Some(Nil),
    serde_library: Option[String] = None,
    inputformat: Option[String] = None,
    outputformat: Option[String] = None,
    storage_properties: Option[Map[String, String]] = None,
    partition_provider: Option[String] = None,
    partition_columns: Option[List[String]] = Some(Nil),
    partition_values: Option[Map[String, String]] = None,
    view_text: Option[String] = None,
    view_original_text: Option[String] = None,
    view_schema_mode: Option[String] = None,
    view_catalog_and_namespace: Option[String] = None,
    view_query_output_columns: Option[List[String]] = None
)

/** Used for columns field of DescribeTableJson */
case class TableColumn(
    name: String,
    `type`: Type,
    element_nullable: Boolean = true,
    comment: Option[String] = None,
    default: Option[String] = None
)

case class Type(
    name: String,
    fields: Option[List[Field]] = None,
    `type`: Option[Type] = None,
    element_type: Option[Type] = None,
    key_type: Option[Type] = None,
    value_type: Option[Type] = None,
    comment: Option[String] = None,
    default: Option[String] = None,
    element_nullable: Option[Boolean] = Some(true),
    value_nullable: Option[Boolean] = Some(true),
    nullable: Option[Boolean] = Some(true)
)

case class Field(
    name: String,
    `type`: Type,
    element_nullable: Boolean = true,
    comment: Option[String] = None,
    default: Option[String] = None
)
