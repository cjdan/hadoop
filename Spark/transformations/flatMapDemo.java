package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;


public class flatMapDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("flatMapDemo");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("hdfs://server1:9000/user/south/input/Test.txt");
        JavaRDD<String> flatMapResult = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
//        JavaRDD<String> flatMapResult = lines.flatMap(new FlatMapFunction<String, String>() {
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Iterator<String> call(String s) {
//
//                return Arrays.asList(s.split(" ")).iterator();
//            }
//
//        });
        flatMapResult.foreach(new VoidFunction<String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(String t) {
                System.out.println(t);
            }
        });

        jsc.stop();
    }
}
