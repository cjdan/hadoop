package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class joinDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("joinDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(0, "aa"),
                new Tuple2<>(1, "a"),
                new Tuple2<>(2, "b"),
                new Tuple2<>(3, "c")
        ),2);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(1, 100),
                new Tuple2<>(2, 200),
                new Tuple2<>(3, 300),
                new Tuple2<>(4, 400)
        ),3);
//        JavaPairRDD<Integer, Tuple2<Integer, String>> join = scoreRDD.join(nameRDD);
//        System.out.println("join.partitions().size()--------"+join.partitions().size());
//
//        join.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Integer, String>>>() {
//            @Override
//            public void call(Tuple2<Integer, Tuple2<Integer, String>> integerTuple2Tuple2) {
//                System.out.println(integerTuple2Tuple2);
//            }
//
//
//
////            @Override
////            public void call(Tuple2<Integer, Tuple2<String, Integer>> integerTuple2Tuple2) {
////                System.out.println(integerTuple2Tuple2);
////            }
//        });


//        JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> leftOuterJoin = nameRDD.leftOuterJoin(scoreRDD);//左外连接
//        leftOuterJoin.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Optional<Integer>>>>() {
//            @Override
//            public void call(Tuple2<Integer, Tuple2<String, Optional<Integer>>> integerTuple2Tuple2) {
//                System.out.println("integerTuple2Tuple2 = " +integerTuple2Tuple2);
//            }
//        });
//        JavaPairRDD<Integer, Tuple2<Optional<String>, Integer>> rightOuterJoin = nameRDD.rightOuterJoin(scoreRDD);//右外连接
//        rightOuterJoin.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Optional<String>, Integer>>>() {
//            @Override
//            public void call(Tuple2<Integer, Tuple2<Optional<String>, Integer>> integerTuple2Tuple2) throws Exception {
//                System.out.println("integerTuple2Tuple2 = "+integerTuple2Tuple2);
//            }
//        });
//        JavaPairRDD<Integer, Tuple2<Optional<String>, Optional<Integer>>> fullOuterJoin = nameRDD.fullOuterJoin(scoreRDD);//全部的key
//        fullOuterJoin.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Optional<String>, Optional<Integer>>>>() {
//            @Override
//            public void call(Tuple2<Integer, Tuple2<Optional<String>, Optional<Integer>>> integerTuple2Tuple2) throws Exception {
//                System.out.println("integerTuple2Tuple2 = "+integerTuple2Tuple2);
//            }
//        });
        sc.stop();
    }
}
