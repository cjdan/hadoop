package Spark.SQL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

public class CreateDataSetFromJDBC {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("CreateDataFrameFromJDBC");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        /*
         * 使用jdbc创建DataFrame的第一种方式
         */
        Map<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://server1:3306/test");
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("user","root");
        options.put("password", "south189");
        options.put("dbtable", "test_data1");
        Dataset<Row> load = sqlContext.read().format("jdbc").options(options).load();
//        load.show();
        load.registerTempTable("test_data1");
//        options.put("dbtable", "score");
//        Dataset<Row> load2 = sqlContext.read().format("jdbc").options(options).load();
//        load2.show();
//        System.out.println("--------------------");
//        load2.registerTempTable("score");
        Dataset<Row> sql = sqlContext.sql("select count(*) from test_data1 where city='吉林' and action = 'Buy'");

//        Dataset<Row> sql = sqlContext.sql("select t1.name,t1.age,t2.chinese,t2.english "
//                + "from person t1,score t2 "
//                + "where t1.id = t2.id ");
        sql.show();
//
//		/**
//		 * 使用jdbc创建DataFrame的第二种方式
//		 */
//		DataFrameReader reader = sqlContext.read().format("jdbc");
//		reader.option("url", "jdbc:mysql://node2:3306/mytest");
//		reader.option("driver", "com.mysql.jdbc.Driver");
//		reader.option("user", "root");
//		reader.option("password", "123456");
//		reader.option("dbtable", "person");
//		DataFrame load3 = reader.load();
//		reader.option("dbtable", "score");
//		DataFrame load4 = reader.load();
//		load3.registerTempTable("person");
//		load4.registerTempTable("score");
//
//		sqlContext.sql("select person.name,person.age,score.english "
//				+ "from person,score "
//				+ "where person.id = score.id").show();
    }
}
