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
package example.Datasources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import example.model.Cube;
import example.model.Square;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaSQLDataSourceExample {



  public static void runGenericFileSourceOptionsExample(SparkSession spark) {
    spark.sql("set spark.sql.files.ignoreCorruptFiles=true");
    Dataset<Row> testCorruptDF = spark.read().parquet("/home/rohtek/IdeaProjects/Spark-Hive-UDF's/src/main/resources/dir1/", "/home/rohtek/IdeaProjects/Spark-Hive-UDF's/src/main/resources/dir1/dir2/");
    testCorruptDF.show();
    Dataset<Row> recursiveLoadedDF = spark.read().format("parquet")
            .option("recursiveFileLookup", "true")
            .load("/home/rohtek/IdeaProjects/Spark-Hive-UDF's/src/main/resources/dir1");
    recursiveLoadedDF.show();
    spark.sql("set spark.sql.files.ignoreCorruptFiles=true");
    Dataset<Row> testGlobFilterDF = spark.read().format("parquet")
            .option("pathGlobFilter", "*.parquet")
            .load("/home/rohtek/IdeaProjects/Spark-Hive-UDF's/src/main/resources/dir1");
    testGlobFilterDF.show();

  }

  public static void runBasicDataSourceExample(SparkSession spark) {

    Dataset<Row> usersDF = spark.read().load("examples/src/main/resources/users.parquet");
    usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");
    Dataset<Row> peopleDF =
      spark.read().format("json").load("examples/src/main/resources/people.json");
    peopleDF.select("name", "age").write().format("parquet").save("namesAndAges.parquet");
    Dataset<Row> peopleDFCsv = spark.read().format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("examples/src/main/resources/people.csv");
    usersDF.write().format("orc")
      .option("orc.bloom.filter.columns", "favorite_color")
      .option("orc.dictionary.key.threshold", "1.0")
      .option("orc.column.encoding.direct", "name")
      .save("users_with_options.orc");
    Dataset<Row> sqlDF =
      spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`");
    peopleDF.write().bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed");
    usersDF
      .write()
      .partitionBy("favorite_color")
      .format("parquet")
      .save("namesPartByColor.parquet");
    peopleDF
      .write()
      .partitionBy("favorite_color")
      .bucketBy(42, "name")
      .saveAsTable("people_partitioned_bucketed");

    spark.sql("DROP TABLE IF EXISTS people_bucketed");
    spark.sql("DROP TABLE IF EXISTS people_partitioned_bucketed");
  }



  public static Dataset runBasicCsvExample(SparkSession spark) {
    Dataset<Row> peopleDFCsv = spark.read().format("csv")
            .option("sep", ",")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("/home/rohtek/IdeaProjects/Spark-Hive-UDF's/src/main/resources/people.csv");

return peopleDFCsv;
    }









  public static Dataset runBasicParquetExample(SparkSession spark) {

    //Dataset<Row> peopleDF = spark.read().json("examples/src/main/resources/people.json");

    //peopleDF.write().parquet("/home/rohtek/IdeaProjects/Spark-Hive-UDF's/people.parquet");

    Dataset<Row> parquetFileDF = spark.read().parquet("/home/rohtek/IdeaProjects/Spark-Hive-UDF's/src/main/resources/user.parquet");
/*
    parquetFileDF.createOrReplaceTempView("parquetFile");
    Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
    Dataset<String> namesDS = namesDF.map(
        (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
        Encoders.STRING());
    namesDS.show();*/

    return parquetFileDF;
  }

  public static Dataset runParquetSchemaMergingExample(SparkSession spark) {
    List<Square> squares = new ArrayList<>();
    for (int value = 1; value <= 5; value++) {
      Square square = new Square();
      square.setValue(value);
      square.setSquare(value * value);
      squares.add(square);
    }

    // Create a simple DataFrame, store into a partition directory
    Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
    squaresDF.write().parquet("data/test_table/key=1");

    List<Cube> cubes = new ArrayList<>();
    for (int value = 6; value <= 10; value++) {
      Cube cube = new Cube();
      cube.setValue(value);
      cube.setCube(value * value * value);
      cubes.add(cube);
    }

    Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
    cubesDF.write().parquet("data/test_table/key=2");

    Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("data/test_table");
    mergedDF.printSchema();
    return mergedDF;

  }

  public static Dataset runJsonDatasetExample(SparkSession spark) {
    Dataset<Row> people = spark.read().json("/home/rohtek/IdeaProjects/Spark-Hive-UDF's/src/main/resources/people.json");

    people.printSchema();
    people.createOrReplaceTempView("people");

    Dataset<Row> namesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
    namesDF.show();
    List<String> jsonData = Arrays.asList(
            "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
    Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
    Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);
    anotherPeople.show();
     return people;
  }

  public static void runJdbcDatasetExample(SparkSession spark) {
    Dataset<Row> jdbcDF = spark.read()
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .load();

    Properties connectionProperties = new Properties();
    connectionProperties.put("user", "username");
    connectionProperties.put("password", "password");
    Dataset<Row> jdbcDF2 = spark.read()
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
 jdbcDF.write()
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save();

    jdbcDF2.write()
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
    jdbcDF.write()
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
  }
}
