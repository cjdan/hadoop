package Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RemoteSubmitApp {
    public static void main(String[] args) {
        // 设置提交任务的用户
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkConf conf = new SparkConf()
                .setAppName("WordCountDemo")
                .setMaster("yarn")
//                .set("spark.jars", "file:///E:/Hadoop_project/target/Hadoop_project-1.0-SNAPSHOT.jar")
                .set("spark.executor.memory", "1024M")
                .set("spark.executor.instance","1")
                .set("yarn.resourcemanager.hostname","server1")
//                .set("spark.driver.host", "10.4.100.225")
                .set("spark.yarn.jars","hdfs://server1:9000/SparkJars/*");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("hdfs://server1:9000/user/south/input/Test.txt");
        JavaRDD<String> flatMap = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(x -> new Tuple2<>(x, 1));
        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(Integer::sum);
        JavaPairRDD<Integer, String> sortByKey = reduceByKey.mapToPair(v -> new Tuple2<>(v._2, v._1)).sortByKey(false);
        JavaPairRDD<String, Integer> res = sortByKey.mapToPair(v -> new Tuple2<>(v._2, v._1));
        res.saveAsTextFile("hdfs://server1:9000/user/south/output/");

        List<Tuple2<String, Integer>> result = res.collect();

        System.out.println(result);
        jsc.stop();

    }
}
