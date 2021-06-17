package Spark.Streaming;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class StreamingBroadcastAccumulatorDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("StreamingBroadcastAccumulatorDemo");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        /*
         * 定义广播变量
         * 下面模拟将a,b广播出去，然后在读取到的文件中过滤显示结果
         */
        List<String> asList = Arrays.asList("matlab","python");

        final Broadcast<List<String>> blackList = jsc.sparkContext().broadcast(asList);
        /*
         * 定义累加器
         * 下面模拟在读取文件的同时，a,b一共被过滤了多少次
         */
        Accumulator<Integer> accumulator = jsc.sparkContext().accumulator(0, "accumulator");

        JavaDStream<String> textFileStream = jsc.textFileStream("file:///E:/data/data1/");
        System.out.println("*********************");

        System.out.println("*********************");

        JavaDStream<String> lines = textFileStream.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaDStream<String> prints = lines.map((Function<String, String>) v1 -> {
            System.out.println("v1 = " + v1);
            return v1;
        });

        JavaPairDStream<String, Integer> mapToPair = prints.mapToPair(new PairFunction<String, String, Integer>() {

            /*
             *
             */
            private static final long serialVersionUID = 1L;

            public Tuple2<String, Integer> call(String arg0) {
                System.out.println("arg0 = "+arg0);

                return new Tuple2<>(arg0.trim(), 1);
            }
        });
        JavaPairDStream<String, Integer> reduceByKey = mapToPair.reduceByKey(Integer::sum);
        reduceByKey.print();
        mapToPair.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            public void call(JavaPairRDD<String, Integer> pairRdd) {
                pairRdd.foreach(t->System.out.println("t= "+t));
                final List<String> black = blackList.getValue();
                if(!pairRdd.isEmpty()){
                    pairRdd.foreach(new VoidFunction<Tuple2<String,Integer>>() {

                        /**
                         *
                         */
                        private static final long serialVersionUID = 1L;

                        public void call(Tuple2<String, Integer> tuple) {
                            if(black.contains(tuple._1)){
                                accumulator.add(tuple._2);
                            }
                        }
                    });
                    /*
                     * 注意：在task中不能读取accumulator的值
                     */
                    System.out.println("accumulator value is :"+accumulator.value());
                }
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
