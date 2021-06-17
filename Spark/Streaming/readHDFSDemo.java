package Spark.Streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class readHDFSDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyDemo");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaDStream<String> lines = jsc.textFileStream("hdfs://server1:9000/user/south/input/Test.txt");
        JavaDStream<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String, Integer> ones = words.mapToPair(x -> new Tuple2<>(x, 1));
        JavaPairDStream<String, Integer> counts = ones.reduceByKey(Integer::sum);
        counts.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.stop();



    }
}
