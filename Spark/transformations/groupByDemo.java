package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class groupByDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("groupByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        System.out.println("Array = "+Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 2),
                new Tuple2<>("b", 3),
                new Tuple2<>("c", 4),
                new Tuple2<>("d", 5),
                new Tuple2<>("d", 6)
        ));
        JavaPairRDD<String, Integer> parallelizePairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 2),
                new Tuple2<>("b", 3),
                new Tuple2<>("c", 4),
                new Tuple2<>("d", 5),
                new Tuple2<>("d", 6)
        ));

        JavaPairRDD<String, Iterable<Integer>> groupByKey = parallelizePairs.groupByKey();
        List<Tuple2<String, Iterable<Integer>>> res = groupByKey.collect();
        System.out.println(res);
        groupByKey.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) {
                System.out.println(t);
            }
        });
    }
}
