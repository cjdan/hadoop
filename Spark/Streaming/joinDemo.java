package Spark.Streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class joinDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("joinDemo");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));


        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("server2", 9999);
        JavaReceiverInputDStream<String> lines1 = jsc.socketTextStream("server3", 9999);
        JavaDStream<String> window = lines.window(Durations.seconds(15), Durations.seconds(5));
        JavaDStream<String> window1 = lines1.window(Durations.seconds(10), Durations.seconds(5));
        JavaPairDStream<String, Integer> words = window.flatMap(s -> Arrays.asList(s.split(" ")).iterator()).mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> words1 = window1.flatMap(s -> Arrays.asList(s.split(" ")).iterator()).mapToPair(s -> new Tuple2<>(s, 1));
//        words.join(words1).print();
        JavaPairDStream<String, Tuple2<Integer, Integer>> res = words.join(words1).reduceByKey((Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>) (v1, v2) -> {
            int num1 = v1._1 + v1._2;
            int num2 = v2._1 + v2._2;//个数
            System.out.println("num1 = "+num1+",num2 = "+num2);
            return new Tuple2<>(num1, num2);
        });
        res.print();

//        JavaDStream<Tuple2<String, Tuple2<Integer, Integer>>> res = words.join(words1)
//                .reduce((Function2<Tuple2<String, Tuple2<Integer, Integer>>, Tuple2<String, Tuple2<Integer, Integer>>, Tuple2<String, Tuple2<Integer, Integer>>>) (v1, v2) -> {
//                    String firstWords = v1._1;
//                    String secondWords = v2._1;
//                    String result;
//                    int num;
//                    int num1;
//                    if(firstWords.equals(secondWords)){
//
//                        result = firstWords;
//                        num = v1._2._1+v1._2._2;
//                        num1 = v2._2._1+v2._2._2;
//
//                    }else {
//                        result = firstWords +" : "+ secondWords;
//                        num = v1._2._1+v1._2._2;
//                        num1 = v2._2._1+v2._2._2;
//                    }
//                    System.out.println("res = " + new Tuple2<>(result,new Tuple2<>(num,num1)));
//
//                    return new Tuple2<>(result,new Tuple2<>(num,num1));
//                });
//        res.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
