package example.util;

import example.Config.ConfConstants;
import org.apache.spark.sql.SparkSession;

public class SparkSessionObj {


   public  static SparkSession  getSession() {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", ConfConstants.warehouseloc)
                .master("local[*]")
                .getOrCreate();

        return spark;
    }
}