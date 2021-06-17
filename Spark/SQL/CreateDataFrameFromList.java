package Spark.SQL;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;


public class CreateDataFrameFromList {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CreateDataFrameFromList");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        List<Person> list = new ArrayList<>();
        for(int i=0;i<10;i++){
            Person person = new Person();
            person.setId(i);
            person.setName("Zhang San"+i);
            person.setAge(2*i);
            person.setGender("gender"+i);
            list.add(person);
        }

        Dataset<Row> df = sqlContext.createDataFrame(list, Person.class);
        df.registerTempTable("person");
        sqlContext.sql("select * from person where age >10 ").show();

    }
}
