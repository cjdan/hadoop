package Spark.Streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class unionDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("unionDemo");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));


        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("server2", 9999);
        JavaReceiverInputDStream<String> lines1 = jsc.socketTextStream("server3", 999);
        JavaDStream<String> window = lines.window(Durations.seconds(15), Durations.seconds(5));
        JavaDStream<String> window1 = lines1.window(Durations.seconds(10), Durations.seconds(5));
        JavaDStream<String> input = window.union(window1);
        JavaDStream<String> flatMap = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String, Integer> mapToPair = flatMap.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> reduceByKey = mapToPair.reduceByKey(Integer::sum);
        reduceByKey.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
