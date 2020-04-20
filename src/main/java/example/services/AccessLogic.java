package example.services;

import example.model.Cube;
import example.model.Record;
import example.model.Square;
import example.util.SparkSessionObj;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import example.Datasources.JavaSQLDataSourceExample;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.classpath.icedtea.Config;
import example.Config.ConfConstants;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class AccessLogic {
public static void main(String args[])
{

    SparkSession spark= SparkSessionObj.getSession();
    String fileformat=ConfConstants.fileformat;
    //JavaSQLDataSourceExample.runBasicDataSourceExample(spark);
    //JavaSQLDataSourceExample.runGenericFileSourceOptionsExample(spark);
    /*
    JavaSQLDataSourceExample.runBasicParquetExample(spark);
    JavaSQLDataSourceExample.runParquetSchemaMergingExample(spark);
    Dataset<Row> df=JavaSQLDataSourceExample.runJsonDatasetExample(spark);
    JavaSQLDataSourceExample.runJdbcDatasetExample(spark);
*/

    Dataset<Row> ds=spark.emptyDataFrame();

switch(fileformat){

    case "csv" :
       ds= JavaSQLDataSourceExample.runBasicCsvExample(spark);
        break;


    case "json" :
        ds=JavaSQLDataSourceExample.runJsonDatasetExample(spark);
         break;

    case "parquet" :
        ds=JavaSQLDataSourceExample.runBasicParquetExample(spark);
        break;

        default :
            ds= JavaSQLDataSourceExample.runBasicCsvExample(spark);

}


ds.show();


Dataset<Row> dfw=ds.withColumn("Id",concat(col("name"),lit('_'),monotonically_increasing_id()));


dfw.show();



UDF1<String,String>  uf= new UDF1<String,String>()
    {
 public String call(String s) throws Exception
 {
      if(s.equals("Developer"))
      {
          return "Incentives";

      }
      else {
     return "NoIncentives";
      }
      }

        };
spark.udf().register("ids",uf, DataTypes.StringType);

Dataset<Row> newdf=dfw.withColumn("Commision",callUDF("ids",col("job")) );

newdf.show();

//dfw.write().bucketBy(6, "Id").sortBy("sal").saveAsTable("test_odm_team.Sample_data_Bucketed");

   // creating new Table in HIVE using Spark

    spark.sql("CREATE TABLE IF NOT EXISTS Keys_Table (key INT, value STRING) ");
    spark.sql("LOAD DATA LOCAL INPATH '/home/rohtek/IdeaProjects/Spark-Hive-UDF's/src/main/resources/kv1.txt' INTO TABLE Keys_Table");

    // Queries are expressed in HiveQL
    spark.sql("SELECT * FROM Keys_Table").show();
    spark.sql("SELECT COUNT(*) FROM Keys_Table").show();
    Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM Keys_Table WHERE key < 10 ORDER BY key");

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

    spark.sql("SELECT * FROM records r JOIN Keys_Table s ON r.key = s.key").show();

    spark.stop();


}


}
