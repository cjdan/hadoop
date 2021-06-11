package Spark.Streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCountDemo");
        /*
         * 在创建streamingContext的时候 设置batch Interval
         */
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		JavaStreamingContext jsc = new JavaStreamingContext(sc,Durations.seconds(5));
//		JavaSparkContext sparkContext = jsc.sparkContext();

        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("server2", 9999);
        JavaDStream<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String, Integer> ones = words.mapToPair(x -> new Tuple2<>(x, 1));
        JavaPairDStream<String, Integer> counts = ones.reduceByKey(Integer::sum);
//        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Iterator<String> call(String s) {
//                return Arrays.asList(s.split(" ")).iterator();
//            }
//        });
//
//        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Tuple2<String, Integer> call(String s) {
//                return new Tuple2<>(s, 1);
//            }
//        });
//
//        JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Integer call(Integer i1, Integer i2) {
//                return i1 + i2;
//            }
//        });
        counts.print();
        jsc.start();
        //等待spark程序被终止
        jsc.awaitTermination();
        jsc.stop(false);
    }
}
