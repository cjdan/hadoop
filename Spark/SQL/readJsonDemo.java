package Spark.SQL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class readJsonDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("readJsonDemo").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        /*schema = StructType([
    StructField("id",LongType(),True),
    StructField("name",StringType(),True),
    StructField("age",LongType(),False),
    StructField("eyeColor",StringType(),True),])
        * */
        Dataset<Row> json = sqlContext.read().json("file:///E:/data/json.txt").filter((FilterFunction<Row>) value -> false);
        json.printSchema();
        json.show();

    }
}
