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

  test("DESCRIBE TABLE EXTENDED AS JSON of a partitioned table") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict;")
      spark.sql(
        s"CREATE TABLE $tbl (id bigint, data array<string>, age int DEFAULT 2) $defaultUsing" +
        " PARTITIONED BY (id)" +
        " COMMENT 'this is a test table'" +
        " LOCATION 'file:/tmp/testcat/table_name'")
      spark.sql(
        s"INSERT INTO $tbl (id, data, age) VALUES(1, ARRAY('a'), 2)")
      val descriptionDf = spark.sql(s"DESCRIBE TABLE EXTENDED $tbl PARTITION (id='1') AS JSON")
      println("\n ************* \n ")
      val firstRow = descriptionDf.select("col_name").head()
      val jsonValue = firstRow.getString(0)
      println(jsonValue)
      checkKeywordsExist(descriptionDf,
        "\"columns\":[{\"id\":0,\"name\":\"data\"," +
          "\"data_type\":\"string\",\"comment\":null}," +
          "{\"id\":1,\"name\":\"id\",\"data_type\":\"bigint\",\"comment\":null}]",
        "\"partition_information\":[{\"id\":0,\"name\":\"id\"," +
          "\"data_type\":\"bigint\",\"comment\":null}],",
        "\"detailed_table_information\":{\"catalog\":\"",
        SESSION_CATALOG_NAME,
        "\"database\":\"ns\",\"table\":\"table\"",
        TableCatalog.PROP_OWNER,
        Utils.getCurrentUserName(),
        "last_access\":\"UNKNOWN\"",
        "type\":\"EXTERNAL\",\"provider\":\"",
        getProvider(),
        "\"comment\":\"this is a test table\",\"table_properties\":",
        "\"location\":\"file:/tmp/testcat/table_name\"," +
        "\"serde_library\":\"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\"," +
        "\"inputformat\":\"org.apache.hadoop.mapred.TextInputFormat\",\"outputformat\":" +
        "\"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\",\"storage_properties\":" +
        "\"[serialization.format=1]\",\"partition_provider\":\"Catalog\"}")
    }
  }

  test("DESCRIBE AS JSON golden file test 1") {
    withNamespaceAndTable("ns", "table") { tbl =>
      val tableCreationStr =
        """
          |CREATE TABLE t (a STRING, b INT, c STRING, d STRING) USING parquet
          |  OPTIONS (a '1', b '2', password 'password')
          |  PARTITIONED BY (c, d) CLUSTERED BY (a) SORTED BY (b ASC) INTO 2 BUCKETS
          |  COMMENT 'table_comment'
          |  TBLPROPERTIES (t 'test', password 'password')
          |""".stripMargin
      spark.sql(tableCreationStr)
      val descriptionDf = spark.sql(s"DESCRIBE t AS JSON")
      println("\n ************* \n ")
      val firstRow = descriptionDf.select("col_name").head()
      val jsonValue = firstRow.getString(0)
      println(jsonValue)
      checkKeywordsExist(descriptionDf,
        "\"columns\":[{\"id\":0,\"name\":\"data\"," +
          "\"data_type\":\"string\",\"comment\":null}," +
          "{\"id\":1,\"name\":\"id\",\"data_type\":\"bigint\",\"comment\":null}]",
        "\"partition_information\":[{\"id\":0,\"name\":\"id\"," +
          "\"data_type\":\"bigint\",\"comment\":null}],",
        "\"detailed_table_information\":{\"catalog\":\"",
        SESSION_CATALOG_NAME,
        "\"database\":\"ns\",\"table\":\"table\"",
        TableCatalog.PROP_OWNER,
        Utils.getCurrentUserName(),
        "last_access\":\"UNKNOWN\"",
        "type\":\"EXTERNAL\",\"provider\":\"",
        getProvider(),
        "\"comment\":\"this is a test table\",\"table_properties\":",
        "\"location\":\"file:/tmp/testcat/table_name\"," +
          "\"serde_library\":\"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\"," +
          "\"inputformat\":\"org.apache.hadoop.mapred.TextInputFormat\",\"outputformat\":" +
          "\"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\",\"storage_properties\":" +
          "\"[serialization.format=1]\",\"partition_provider\":\"Catalog\"}")
    }
  }

  test("DESCRIBE AS JSON golden file test 2") {
    withNamespaceAndTable("ns", "table") { tbl =>
      val tableCreationStr =
        """
          |CREATE TABLE t (a STRING, b INT, c STRING, d STRING) USING parquet
          |  OPTIONS (a '1', b '2', password 'password')
          |  PARTITIONED BY (c, d) CLUSTERED BY (a) SORTED BY (b ASC) INTO 2 BUCKETS
          |  COMMENT 'table_comment'
          |  TBLPROPERTIES (t 'test', password 'password')
          |""".stripMargin
      spark.sql(tableCreationStr)
      spark.sql("ALTER TABLE t ADD PARTITION (c='Us', d=1)")
      val descriptionDf = spark.sql(s"DESCRIBE t PARTITION (c='Us', d=1) AS JSON")
      println("\n ************* \n ")
      val firstRow = descriptionDf.select("col_name").head()
      val jsonValue = firstRow.getString(0)
      println(jsonValue)
      checkKeywordsExist(descriptionDf,
        "\"columns\":[{\"id\":0,\"name\":\"data\"," +
          "\"data_type\":\"string\",\"comment\":null}," +
          "{\"id\":1,\"name\":\"id\",\"data_type\":\"bigint\",\"comment\":null}]",
        "\"partition_information\":[{\"id\":0,\"name\":\"id\"," +
          "\"data_type\":\"bigint\",\"comment\":null}],",
        "\"detailed_table_information\":{\"catalog\":\"",
        SESSION_CATALOG_NAME,
        "\"database\":\"ns\",\"table\":\"table\"",
        TableCatalog.PROP_OWNER,
        Utils.getCurrentUserName(),
        "last_access\":\"UNKNOWN\"",
        "type\":\"EXTERNAL\",\"provider\":\"",
        getProvider(),
        "\"comment\":\"this is a test table\",\"table_properties\":",
        "\"location\":\"file:/tmp/testcat/table_name\"," +
          "\"serde_library\":\"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\"," +
          "\"inputformat\":\"org.apache.hadoop.mapred.TextInputFormat\",\"outputformat\":" +
          "\"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\",\"storage_properties\":" +
          "\"[serialization.format=1]\",\"partition_provider\":\"Catalog\"}")
    }
  }

  test("DESCRIBE AS JSON golden file test 3") {
    withNamespaceAndTable("ns", "table") { tbl =>
      val tableCreationStr =
        """
          |CREATE TABLE d (a STRING DEFAULT 'default-value', b INT DEFAULT 42)
          |USING parquet COMMENT 'table_comment'
          |""".stripMargin
      spark.sql(tableCreationStr)
      val descriptionDf = spark.sql(s"DESC d AS JSON")
      println("\n ************* \n ")
      val firstRow = descriptionDf.select("col_name").head()
      val jsonValue = firstRow.getString(0)
      println(jsonValue)
      checkKeywordsExist(descriptionDf,
        "\"columns\":[{\"id\":0,\"name\":\"data\"," +
          "\"data_type\":\"string\",\"comment\":null}," +
          "{\"id\":1,\"name\":\"id\",\"data_type\":\"bigint\",\"comment\":null}]",
        "\"partition_information\":[{\"id\":0,\"name\":\"id\"," +
          "\"data_type\":\"bigint\",\"comment\":null}],",
        "\"detailed_table_information\":{\"catalog\":\"",
        SESSION_CATALOG_NAME,
        "\"database\":\"ns\",\"table\":\"table\"",
        TableCatalog.PROP_OWNER,
        Utils.getCurrentUserName(),
        "last_access\":\"UNKNOWN\"",
        "type\":\"EXTERNAL\",\"provider\":\"",
        getProvider(),
        "\"comment\":\"this is a test table\",\"table_properties\":",
        "\"location\":\"file:/tmp/testcat/table_name\"," +
          "\"serde_library\":\"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\"," +
          "\"inputformat\":\"org.apache.hadoop.mapred.TextInputFormat\",\"outputformat\":" +
          "\"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\",\"storage_properties\":" +
          "\"[serialization.format=1]\",\"partition_provider\":\"Catalog\"}")
    }
  }

  test("DESCRIBE AS JSON golden file test 4") {
    withNamespaceAndTable("ns", "table") { tbl =>
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
      println("\n ************* \n ")
      val firstRow = descriptionDf.select("col_name").head()
      val jsonValue = firstRow.getString(0)
      println(jsonValue)
      checkKeywordsExist(descriptionDf,
        "\"columns\":[{\"id\":0,\"name\":\"data\"," +
          "\"data_type\":\"string\",\"comment\":null}," +
          "{\"id\":1,\"name\":\"id\",\"data_type\":\"bigint\",\"comment\":null}]",
        "\"partition_information\":[{\"id\":0,\"name\":\"id\"," +
          "\"data_type\":\"bigint\",\"comment\":null}],",
        "\"detailed_table_information\":{\"catalog\":\"",
        SESSION_CATALOG_NAME,
        "\"database\":\"ns\",\"table\":\"table\"",
        TableCatalog.PROP_OWNER,
        Utils.getCurrentUserName(),
        "last_access\":\"UNKNOWN\"",
        "type\":\"EXTERNAL\",\"provider\":\"",
        getProvider(),
        "\"comment\":\"this is a test table\",\"table_properties\":",
        "\"location\":\"file:/tmp/testcat/table_name\"," +
          "\"serde_library\":\"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\"," +
          "\"inputformat\":\"org.apache.hadoop.mapred.TextInputFormat\",\"outputformat\":" +
          "\"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\",\"storage_properties\":" +
          "\"[serialization.format=1]\",\"partition_provider\":\"Catalog\"}")
    }
  }

  test("DESCRIBE AS JSON golden file test 5") {
    withNamespaceAndTable("ns", "table") { tbl =>
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
      println("\n ************* \n ")
      val firstRow = descriptionDf.select("col_name").head()
      val jsonValue = firstRow.getString(0)
      println(jsonValue)
      checkKeywordsExist(descriptionDf,
        "\"columns\":[{\"id\":0,\"name\":\"data\"," +
          "\"data_type\":\"string\",\"comment\":null}," +
          "{\"id\":1,\"name\":\"id\",\"data_type\":\"bigint\",\"comment\":null}]",
        "\"partition_information\":[{\"id\":0,\"name\":\"id\"," +
          "\"data_type\":\"bigint\",\"comment\":null}],",
        "\"detailed_table_information\":{\"catalog\":\"",
        SESSION_CATALOG_NAME,
        "\"database\":\"ns\",\"table\":\"table\"",
        TableCatalog.PROP_OWNER,
        Utils.getCurrentUserName(),
        "last_access\":\"UNKNOWN\"",
        "type\":\"EXTERNAL\",\"provider\":\"",
        getProvider(),
        "\"comment\":\"this is a test table\",\"table_properties\":",
        "\"location\":\"file:/tmp/testcat/table_name\"," +
          "\"serde_library\":\"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\"," +
          "\"inputformat\":\"org.apache.hadoop.mapred.TextInputFormat\",\"outputformat\":" +
          "\"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\",\"storage_properties\":" +
          "\"[serialization.format=1]\",\"partition_provider\":\"Catalog\"}")
    }
  }
}
