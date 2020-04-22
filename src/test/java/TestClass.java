import example.model.Employee;
import example.services.MyAverage;
import example.util.SparkSessionObj;
import org.apache.spark.sql.*;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestClass {

    static String avgvalue="";

    @BeforeClass
    public static void setUp() {
        SparkSession spark = SparkSessionObj.getSession();
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String path = "/home/rohtek/IdeaProjects/Spark-Hive-UDF's/src/main/resources/employees.json";
        Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
        ds.show();

        MyAverage myAverage = new MyAverage();
        TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
        Dataset<Double> result = ds.select(averageSalary);
        avgvalue = result.collectAsList().get(0).toString();


        System.out.println("AVg "+avgvalue);
        //result.show();
       // spark.stop();


    }
    @Test
    public void testExample(){



        String exceptedval="3750.0";

        Assert.assertTrue(compareString(avgvalue,exceptedval));

    }


    public boolean compareString(String actualResult,String expectedResult){

        if(actualResult.equals(expectedResult))
        {
            return true;
        }else
        {

            return false;
        }

    }

}