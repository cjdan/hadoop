package Spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCountDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf
//        .setMaster("local")
        .setAppName("WordCountDemo");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> files = jsc.textFile("hdfs://server1:9000/user/south/input/Test.txt");
        JavaRDD<String> lines = files.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> words = lines.mapToPair(x -> new Tuple2<>(x, 1));
        JavaPairRDD<String, Integer> reduce = words.reduceByKey(Integer::sum);
        JavaPairRDD<Integer, String> change = reduce.mapToPair(x -> new Tuple2<>(x._2, x._1));
        JavaPairRDD<Integer, String> sortByKey = change.sortByKey(false);//降序
        JavaPairRDD<String, Integer> res = sortByKey.mapToPair(x -> new Tuple2<>(x._2, x._1));
        List<Tuple2<String, Integer>> result = res.collect();
        System.out.println(result.size());
        jsc.stop();
    }
}
