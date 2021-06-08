package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
/**
 * 当调用类型（K，V）和（K，W）的数据集时，返回一个数据集（K，（Iterable <V>，Iterable <W>））元组。此操作也被称做groupWith。
 * @author root
 *
 */

public class cogroupDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("cogroupDemo").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String,String>> studentsList = Arrays.asList(
                new Tuple2<>("1", "Zhang San"),
                new Tuple2<>("2", "Li Si"),
                new Tuple2<>("2", "Wang Wu"),
                new Tuple2<>("3", "Ma Liu"));
        List<Tuple2<String,String>> scoreList = Arrays.asList(
                new Tuple2<>("1", "100"),
                new Tuple2<>("2", "90"),
                new Tuple2<>("3", "80"),
                new Tuple2<>("1", "1000"),
                new Tuple2<>("2", "60"),
                new Tuple2<>("3", "50"));
        JavaPairRDD<String,String> students = sc.parallelizePairs(studentsList);
        JavaPairRDD<String,String> scores = sc.parallelizePairs(scoreList);
        // cogroup 与 join不同！
        // 相当于，一个key join上所有value，都给放到一个Iterable里面去！
        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> studentScores = students.cogroup(scores);
        //                System.out.println(tuple);
        //                System.out.println(tuple);
        List<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>> res = studentScores.collect();
        System.out.println(res);
        studentScores.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(
                    Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> tuple) {
//                System.out.println(tuple);
                String first = tuple._1;
                Iterable<String> second = tuple._2._1;
                Iterable<String> third = tuple._2._2;
                System.out.println("student id : " + first);
                System.out.println("student name : " + second);
                System.out.println("student score : " + third);
            }
        });

//		System.out.println(studentScores.collect());

        sc.close();
    }

}
