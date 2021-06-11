package Spark.Streaming;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class readCheckpointDemo {

    public static void main(String[] args) throws InterruptedException {
        String CHECK_POINT_PATH = "file:///E:/checkpoint";

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(CHECK_POINT_PATH, (Function0<JavaStreamingContext>) () -> {
            String CHECK_POINT_PATH1 = "file:///E:/checkpoint";
            SparkConf conf = new SparkConf()
                    .setMaster("local[2]") //为什么启动3个，有一个Thread运行Receiver
                    .setAppName("F_CheckPointOrderTotalStreaming");

            JavaStreamingContext jsc1 = new JavaStreamingContext(conf, Durations.seconds(5));
            jsc1.checkpoint(CHECK_POINT_PATH1);
            return jsc1;
        });
        JavaReceiverInputDStream<String> lines =  jsc.socketTextStream("server2",9999);
        JavaPairDStream<String, Integer> res = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator()).mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey(Integer::sum);
        res.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }

}
