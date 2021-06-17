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

public class WindowDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("WindowHotWord");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        /*
         * 设置日志级别为WARN
         *
         */
        jssc.sparkContext().setLogLevel("WARN");
        /*
         * 注意：
         *  没有优化的窗口函数可以不设置checkpoint目录
         *  优化的窗口函数必须设置checkpoint目录
         */
//   		jssc.checkpoint("hdfs://server1:9000/user/spark/checkpoint2");
//        jssc.checkpoint("./checkpoint");
        JavaReceiverInputDStream<String> searchLogsDStream = jssc.socketTextStream("server2", 9999);
        JavaDStream<String> window = searchLogsDStream.window(Durations.seconds(15), Durations.seconds(5));
        JavaDStream<String> words = window.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> counts = ones.reduceByKey(Integer::sum);
        counts.print();
        //word	1
        JavaDStream<String> searchWordsDStream = searchLogsDStream.flatMap(new FlatMapFunction<String, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String t) {
                return Arrays.asList(t.split(" ")).iterator();
            }
        });

        // 将搜索词映射为(searchWord, 1)的tuple格式
        JavaPairDStream<String, Integer> searchWordPairDStream = searchWordsDStream.mapToPair(

                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String searchWord) {
                        return new Tuple2<>(searchWord, 1);
                    }

                });
        /*
         * 每隔10秒，计算最近60秒内的数据，那么这个窗口大小就是60秒，里面有12个rdd，在没有计算之前，这些rdd是不会进行计算的。
         * 那么在计算的时候会将这12个rdd聚合起来，然后一起执行reduceByKeyAndWindow操作 ，
         * reduceByKeyAndWindow是针对窗口操作的而不是针对DStream操作的。
         */
	   	 JavaPairDStream<String, Integer> searchWordCountsDStream =

				searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
		}, Durations.seconds(15), Durations.seconds(5));



        /*
         * window窗口操作优化：
         */
//        JavaPairDStream<String, Integer> searchWordCountsDStream =
//
//                searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
//
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public Integer call(Integer v1, Integer v2) {
//                        return v1 + v2;
//                    }
//
//                },new Function2<Integer, Integer, Integer>() {
//
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public Integer call(Integer v1, Integer v2) {
//                        return v1 - v2;
//                    }
//
//                }, Durations.seconds(15), Durations.seconds(5));

        searchWordCountsDStream.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
