package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class zipDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("zipDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> nameRDD = sc.parallelize(Arrays.asList("Zhang San","Li Si","Wang Wu"));
//        JavaRDD<Integer> scoreRDD = sc.parallelize(Arrays.asList(100,200,300));
////		JavaRDD<Integer> scoreRDD = sc.parallelize(Arrays.asList(100,200,300,400));
//        JavaPairRDD<String, Integer> zip = nameRDD.zip(scoreRDD);
//        zip.foreach(new VoidFunction<Tuple2<String,Integer>>() {
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public void call(Tuple2<String, Integer> tuple) {
//                System.out.println("tuple --- " + tuple);
//            }
//        });

		JavaPairRDD<String, String> parallelizePairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", "aaa"),
                new Tuple2<>("b", "bbb"),
                new Tuple2<>("c", "ccc")
				));
		JavaPairRDD<String, String> parallelizePairs1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("1", "111"),
                new Tuple2<>("2", "222"),
                new Tuple2<>("3", "333")
				));
		JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> result = parallelizePairs.zip(parallelizePairs1);
        result.foreach(new VoidFunction<Tuple2<Tuple2<String, String>, Tuple2<String, String>>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Tuple2<Tuple2<String, String>, Tuple2<String, String>> tuple2Tuple2Tuple2) {
                System.out.println(" tuple2Tuple2Tuple2 ----" + tuple2Tuple2Tuple2);
            }
        });

        sc.stop();
    }
}
