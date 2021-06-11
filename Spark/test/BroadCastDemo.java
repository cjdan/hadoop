package Spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BroadCastDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("BroadCastDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("hello scala","hello matlab");
        //广播变量将list广播出去
        final Broadcast<List<String>> broadCastList = sc.broadcast(list);

        JavaRDD<String> lines = sc.textFile("hdfs://server1:9000/user/south/input/Test.txt");
        JavaRDD<String> result = lines.filter(new Function<String, Boolean>() {

            /*
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(String s) {
                System.out.println("s = " + s);
                System.out.println("broadCastList = "+broadCastList.getValue());
                return broadCastList.value().contains(s);
            }
        });
        result.foreach(new VoidFunction<String>() {

            /*
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(String t) {
                System.out.println("t = "+ t);
            }
        });

        sc.close();
    }
}
