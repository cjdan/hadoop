package Spark.Streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class countDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("unionDemo");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));


        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("server2", 9999);
        JavaDStream<String> window = lines.window(Durations.seconds(15), Durations.seconds(5));
        JavaDStream<Long> count = window.count();
        count.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
