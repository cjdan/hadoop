package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class mapToPairDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("mapToPairDemo");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("hdfs://server1:9000/user/south/input/Test.txt");
        JavaRDD<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> result = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String t) {
                return new Tuple2<>(t, 1);
            }
        });
        List<Tuple2<String, Integer>> res = result.collect();
        System.out.println("res = " + res);
        result.foreach(new VoidFunction<Tuple2<String,Integer>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Integer> t) {
                System.out.println(t);
            }
        });

        jsc.stop();
    }
}
