package example.sql.hive;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import example.model.Record;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class JavaSparkHiveExample {

    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .master("local[*]")
                .getOrCreate();

        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
        spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");

        // Queries are expressed in HiveQL
        spark.sql("SELECT * FROM src").show();
        spark.sql("SELECT COUNT(*) FROM src").show();
        Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");

        // The items in DataFrames are of type Row, which lets you to access each column by ordinal.
        Dataset<String> stringsDS =
                sqlDF.map((MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1), Encoders.STRING());
        stringsDS.show();
        List<Record> records = new ArrayList<>();
        for (int key = 1; key < 100; key++) {
            Record record = new Record();
            record.setKey(key);
            record.setValue("val_" + key);
            records.add(record);
        }
        Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
        recordsDF.createOrReplaceTempView("records");

        spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();

        spark.stop();


    }

    }
