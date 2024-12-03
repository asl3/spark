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

package org.apache.spark.sql.hive.execution.command

 import org.json4s._
 import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.execution.command.v1
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Utils

/**
 * The class contains tests for the `DESCRIBE TABLE` command to check V1 Hive external
 * table catalog.
 */
class DescribeTableSuite extends v1.DescribeTableSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[DescribeTableSuiteBase].commandVersion

  implicit val formats: org.json4s.DefaultFormats.type = org.json4s.DefaultFormats

  test("Table Ownership") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (c int) $defaultUsing")
      checkHiveClientCalls(expected = 6) {
        checkAnswer(
          sql(s"DESCRIBE TABLE EXTENDED $t")
            .where("col_name='Owner'")
            .select("col_name", "data_type"),
          Row("Owner", Utils.getCurrentUserName()))
      }
    }
  }


  test("DESCRIBE TABLE EXTENDED of a partitioned table") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing" +
        " PARTITIONED BY (id)" +
        " COMMENT 'this is a test table'" +
        " LOCATION 'file:/tmp/testcat/table_name'")
      val descriptionDf = spark.sql(s"DESCRIBE TABLE EXTENDED $tbl")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      QueryTest.checkAnswer(
        // Filter out 'Table Properties' to don't check `transient_lastDdlTime`
        descriptionDf.filter("!(col_name in ('Created Time', 'Table Properties', 'Created By'))"),
        Seq(
          Row("data", "string", null),
          Row("id", "bigint", null),
          Row("# Partition Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("id", "bigint", null),
          Row("", "", ""),
          Row("# Detailed Table Information", "", ""),
          Row("Catalog", SESSION_CATALOG_NAME, ""),
          Row("Database", "ns", ""),
          Row("Table", "table", ""),
          Row(TableCatalog.PROP_OWNER.capitalize, Utils.getCurrentUserName(), ""),
          Row("Last Access", "UNKNOWN", ""),
          Row("Type", "EXTERNAL", ""),
          Row("Provider", getProvider(), ""),
          Row("Comment", "this is a test table", ""),
          Row("Location", "file:/tmp/testcat/table_name", ""),
          Row("Serde Library", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", ""),
          Row("InputFormat", "org.apache.hadoop.mapred.TextInputFormat", ""),
          Row("OutputFormat", "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat", ""),
          Row("Storage Properties", "[serialization.format=1]", ""),
          Row("Partition Provider", "Catalog", "")))
    }
  }

  test("DESCRIBE AS JSON partitions, clusters, buckets") {
    withNamespaceAndTable("ns", "table") { t =>
      val tableCreationStr =
        s"""
          |CREATE TABLE $t (a STRING, b INT, c STRING, d STRING) USING parquet
          |  OPTIONS (a '1', b '2', password 'password')
          |  PARTITIONED BY (c, d) CLUSTERED BY (a) SORTED BY (b ASC) INTO 2 BUCKETS
          |  COMMENT 'table_comment'
          |  TBLPROPERTIES (t 'test', password 'password')
          |""".stripMargin
      spark.sql(tableCreationStr)
      val descriptionDf = spark.sql(s"DESCRIBE $t AS JSON")
      val firstRow = descriptionDf.select("col_name").head()
      val jsonValue = firstRow.getString(0)
      val parsedOutput = parse(jsonValue).extract[DescribeTableOutput]

      val expectedOutput = DescribeTableOutput(
        table_name = Some("table"),
        catalog_names = Some(List(SESSION_CATALOG_NAME)),
        database_names = Some(List("ns")),
        qualified_name = Some(s"spark_catalog.ns.table"),
        columns = Some(List(
          Column(1, "a", Type("string")),
          Column(2, "b", Type("integer")),
          Column(3, "c", Type("string")),
          Column(4, "d", Type("string"))
        )),
        owner = Some(""),
        created_time = Some(""),
        last_access = Some("UNKNOWN"),
        created_by = Some("Spark 4.0.0-SNAPSHOT"),
        `type` = Some("MANAGED"),
        provider = Some("parquet"),
        bucket_columns = Some(List("a")),
        sort_columns = Some(List("b")),
        comment = Some("table_comment"),
        table_properties = Some(Map(
          "password" -> "*********(redacted)",
          "t" -> "test"
        )),
        location = Some(""),
        serde_library = Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
        inputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
        outputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
        storage_properties = Some(Map(
          "a" -> "1",
          "b" -> "2",
          "password" -> "*********(redacted)"
        )),
        partition_provider = Some("Catalog"),
        partition_columns = Some(List("c", "d"))
      )

      assert(expectedOutput == parsedOutput.copy(owner = Some(""),
        created_time = Some(""),
        location = Some("")))
    }
  }

  test("DESCRIBE AS JSON partition spec") {
    withNamespaceAndTable("ns", "table") { t =>
      val tableCreationStr =
        s"""
          |CREATE TABLE $t (a STRING, b INT, c STRING, d STRING) USING parquet
          |  OPTIONS (a '1', b '2', password 'password')
          |  PARTITIONED BY (c, d) CLUSTERED BY (a) SORTED BY (b ASC) INTO 2 BUCKETS
          |  COMMENT 'table_comment'
          |  TBLPROPERTIES (t 'test', password 'password')
          |""".stripMargin
      spark.sql(tableCreationStr)
      spark.sql(s"ALTER TABLE $t ADD PARTITION (c='Us', d=1)")
      val descriptionDf = spark.sql(s"DESCRIBE $t PARTITION (c='Us', d=1) AS JSON")
      val firstRow = descriptionDf.select("col_name").head()
      val jsonValue = firstRow.getString(0)
      val parsedOutput = parse(jsonValue).extract[DescribeTableOutput]

      val expectedOutput = DescribeTableOutput(
        table_name = Some("table"),
        catalog_names = Some(List("spark_catalog")),
        database_names = Some(List("ns")),
        qualified_name = Some("spark_catalog.ns.table"),
        columns = Some(List(
          Column(1, "a", Type("string")),
          Column(2, "b", Type("integer")),
          Column(3, "c", Type("string")),
          Column(4, "d", Type("string"))
        )),
        last_access = Some("UNKNOWN"),
        created_by = Some("Spark 4.0.0-SNAPSHOT"),
        `type` = Some("MANAGED"),
        provider = Some("parquet"),
        bucket_columns = Some(List("a")),
        sort_columns = Some(List("b")),
        comment = Some("table_comment"),
        table_properties = Some(Map(
          "password" -> "*********(redacted)",
          "t" -> "test"
        )),
        serde_library = Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
        inputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
        outputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
        storage_properties = Some(Map(
          "a" -> "1",
          "serialization.format" -> "1",
          "b" -> "2",
          "password" -> "*********(redacted)"
        )),
        partition_provider = Some("Catalog"),
        partition_columns = Some(List("c", "d")),
        partition_values = Some(Map("c" -> "Us", "d" -> "1"))
      )

      assert(expectedOutput ==
        parsedOutput.copy(location = None, created_time = None, owner = None))
    }
  }

  test("DESCRIBE AS JSON default values") {
    withNamespaceAndTable("ns", "table") { t =>
      val tableCreationStr =
        """
          |CREATE TABLE d (a STRING DEFAULT 'default-value', b INT DEFAULT 42)
          |USING parquet COMMENT 'table_comment'
          |""".stripMargin
      spark.sql(tableCreationStr)
      val descriptionDf = spark.sql(s"DESC d AS JSON")
      val firstRow = descriptionDf.select("col_name").head()
      val jsonValue = firstRow.getString(0)
      val parsedOutput = parse(jsonValue).extract[DescribeTableOutput]

      val expectedOutput = DescribeTableOutput(
        table_name = Some("d"),
        catalog_names = Some(List("spark_catalog")),
        database_names = Some(List("default")),
        qualified_name = Some("spark_catalog.default.d"),
        columns = Some(List(
          Column(1, "a", Type("string"), default_value = Some("'default-value'")),
          Column(2, "b", Type("integer"), default_value = Some("42"))
        )),
        last_access = Some("UNKNOWN"),
        created_by = Some("Spark 4.0.0-SNAPSHOT"),
        `type` = Some("MANAGED"),
        storage_properties = None,
        provider = Some("parquet"),
        bucket_columns = Some(Nil), // No bucket columns in actual JSON
        sort_columns = Some(Nil), // No sort columns in actual JSON
        comment = Some("table_comment"),
        serde_library = Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
        inputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
        outputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
        table_properties = None
      )

      assert(expectedOutput ==
        parsedOutput.copy(location = None, created_time = None, owner = None))
    }
  }

  // TODO: Should temp view have no other fields?
  test("DESCRIBE AS JSON temp view") {
    withNamespaceAndTable("ns", "table") { t =>
      val tableCreationStr =
        """
          |CREATE TABLE t (a STRING, b INT, c STRING, d STRING) USING parquet
          |  OPTIONS (a '1', b '2', password 'password')
          |  PARTITIONED BY (c, d) CLUSTERED BY (a) SORTED BY (b ASC) INTO 2 BUCKETS
          |  COMMENT 'table_comment'
          |  TBLPROPERTIES (t 'test', password 'password')
          |""".stripMargin
      spark.sql(tableCreationStr)
      spark.sql("CREATE TEMPORARY VIEW temp_v AS SELECT * FROM t")
      val descriptionDf = spark.sql(s"DESCRIBE temp_v AS JSON")
      val firstRow = descriptionDf.select("col_name").head()
      val jsonValue = firstRow.getString(0)
      val parsedOutput = parse(jsonValue).extract[DescribeTableOutput]

      val expectedOutput = DescribeTableOutput(
        columns = Some(List(
          Column(1, "a", Type("string")),
          Column(2, "b", Type("integer")),
          Column(3, "c", Type("string")),
          Column(4, "d", Type("string"))
        ))
      )

      assert(expectedOutput == parsedOutput)
    }
  }

  test("DESCRIBE AS JSON complex types") {
    withNamespaceAndTable("ns", "table") { t =>
      val tableCreationStr =
        """
          |CREATE TABLE c (
          |  id STRING,
          |  nested_struct STRUCT<
          |    name: STRING,
          |    age: INT,
          |    contact: STRUCT<
          |      email: STRING,
          |      phone_numbers: ARRAY<STRING>,
          |      addresses: ARRAY<STRUCT<
          |        street: STRING,
          |        city: STRING,
          |        zip: INT
          |      >>
          |    >
          |  >,
          |  preferences MAP<STRING, ARRAY<STRING>>
          |) USING parquet
          |  OPTIONS (option1 'value1', option2 'value2')
          |  PARTITIONED BY (id)
          |  COMMENT 'A table with nested complex types'
          |  TBLPROPERTIES ('property1' = 'value1', 'password' = 'password')
        """.stripMargin
      spark.sql(tableCreationStr)
      val descriptionDf = spark.sql(s"DESCRIBE c AS JSON")
      val firstRow = descriptionDf.select("col_name").head()
      val jsonValue = firstRow.getString(0)
      val parsedOutput = parse(jsonValue).extract[DescribeTableOutput]

      val expectedOutput = DescribeTableOutput(
        table_name = Some("c"),
        catalog_names = Some(List("spark_catalog")),
        database_names = Some(List("default")),
        qualified_name = Some("spark_catalog.default.c"),
        columns = Some(List(
          Column(
            id = 1,
            name = "nested_struct",
            `type` = Type(
              `type` = "struct",
              fields = Some(List(
                Field(
                  name = "name",
                  `type` = Type("string"),
                  nullable = Some(true)
                ),
                Field(
                  name = "age",
                  `type` = Type("integer"),
                  nullable = Some(true)
                ),
                Field(
                  name = "contact",
                  `type` = Type(
                    `type` = "struct",
                    fields = Some(List(
                      Field(
                        name = "email",
                        `type` = Type("string"),
                        nullable = Some(true)
                      ),
                      Field(
                        name = "phone_numbers",
                        `type` = Type(
                          `type` = "array",
                          elementType = Some(Type("string")),
                          containsNull = Some(true)
                        ),
                        nullable = Some(true)
                      ),
                      Field(
                        name = "addresses",
                        `type` = Type(
                          `type` = "array",
                          elementType = Some(Type(
                            `type` = "struct",
                            fields = Some(List(
                              Field(
                                name = "street",
                                `type` = Type("string"),
                                nullable = Some(true)
                              ),
                              Field(
                                name = "city",
                                `type` = Type("string"),
                                nullable = Some(true)
                              ),
                              Field(
                                name = "zip",
                                `type` = Type("integer"),
                                nullable = Some(true)
                              )
                            ))
                          )),
                          containsNull = Some(true)
                        ),
                        nullable = Some(true)
                      )
                    ))
                  ),
                  nullable = Some(true)
                )
              ))
            ),
            default_value = None
          ),
          Column(
            id = 2,
            name = "preferences",
            `type` = Type(
              `type` = "map",
              keyType = Some(Type("string")),
              valueType = Some(Type(
                `type` = "array",
                elementType = Some(Type("string")),
                containsNull = Some(true)
              )),
              valueContainsNull = Some(true)
            ),
            default_value = None
          ),
          Column(
            id = 3,
            name = "id",
            `type` = Type("string"),
            default_value = None
          )
        )),
        serde_library = Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
        inputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
        outputformat = Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
        storage_properties = Some(Map(
          "option1" -> "value1",
          "option2" -> "value2"
        )),
        last_access = Some("UNKNOWN"),
        created_by = Some("Spark 4.0.0-SNAPSHOT"),
        `type` = Some("MANAGED"),
        provider = Some("parquet"),
        comment = Some("A table with nested complex types"),
        table_properties = Some(Map(
          "password" -> "*********(redacted)",
          "property1" -> "value1"
        )),
        partition_provider = Some("Catalog"),
        partition_columns = Some(List("id"))
      )

      assert(expectedOutput ==
        parsedOutput.copy(location = None, created_time = None, owner = None))
    }
  }
}

case class DescribeTableOutput(
  table_name: Option[String] = None,
  catalog_names: Option[List[String]] = Some(Nil),
  database_names: Option[List[String]] = Some(Nil),
  qualified_name: Option[String] = None,
  columns: Option[List[Column]] = Some(Nil),
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
  serde_library: Option[String] = None,
  inputformat: Option[String] = None,
  outputformat: Option[String] = None,
  storage_properties: Option[Map[String, String]] = None,
  partition_provider: Option[String] = None,
  partition_columns: Option[List[String]] = Some(Nil),
  partition_values: Option[Map[String, String]] = None
)

case class Column(
 id: Int,
 name: String,
 `type`: Type,
 default_value: Option[String] = None
)

case class Type(
   `type`: String,
   fields: Option[List[Field]] = None,
   elementType: Option[Type] = None,
   keyType: Option[Type] = None,
   valueType: Option[Type] = None,
   nullable: Option[Boolean] = None,
   containsNull: Option[Boolean] = None,
   valueContainsNull: Option[Boolean] = None
  )

case class Field(
  name: String,
  `type`: Type,
  nullable: Option[Boolean] = None
)
