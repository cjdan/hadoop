package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class zipWithIndexDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("zipWithIndex");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> nameRDD = sc.parallelize(Arrays.asList("Zhang San","Li Si","Wang Wu"));
//        JavaPairRDD<String, Long> zipWithIndex = nameRDD.zipWithIndex();
//        zipWithIndex.foreach(new VoidFunction<Tuple2<String,Long>>() {
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public void call(Tuple2<String, Long> t) {
//                System.out.println("t ---- "+ t);
//            }
//        });
		JavaPairRDD<String, String> parallelizePairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", "aaa"),
                new Tuple2<>("b", "bbb"),
                new Tuple2<>("c", "ccc")
				));
		JavaPairRDD<Tuple2<String, String>, Long> zipWithIndex2 = parallelizePairs.zipWithIndex();
		zipWithIndex2.foreach(new VoidFunction<Tuple2<Tuple2<String,String>,Long>>() {

			/**
			 *
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Tuple2<String, String>, Long> t) {
				System.out.println(" t ----" + t);
			}
		});
        sc.stop();
    }
}
