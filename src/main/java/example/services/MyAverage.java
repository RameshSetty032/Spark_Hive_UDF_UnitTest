package example.services;

import example.model.Average;
import example.model.Employee;
import example.util.SparkSessionObj;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

public class MyAverage extends Aggregator<Employee, Average, Double> {
        public Average zero() {
            return new Average(0L, 0L);
        }
        public Average reduce(Average buffer, Employee employee) {
            long newSum = buffer.getSum() + employee.getSalary();
            long newCount = buffer.getCount() + 1;
            buffer.setSum(newSum);
            buffer.setCount(newCount);
            return buffer;
        }
        public Average merge(Average b1, Average b2) {
            long mergedSum = b1.getSum() + b2.getSum();
            long mergedCount = b1.getCount() + b2.getCount();
            b1.setSum(mergedSum);
            b1.setCount(mergedCount);
            return b1;
        }
        public Double finish(Average reduction) {
            return ((double) reduction.getSum()) / reduction.getCount();
        }
        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }


    public static void main(String[] args) {

        SparkSession spark= SparkSessionObj.getSession();

        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String path = "/home/rohtek/IdeaProjects/Spark-Hive-UDF's/src/main/resources/employees.json";
        Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
        ds.show();

        MyAverage myAverage = new MyAverage();
        TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
        Dataset<Double> result = ds.select(averageSalary);
        result.show();
        spark.stop();
    }

}






