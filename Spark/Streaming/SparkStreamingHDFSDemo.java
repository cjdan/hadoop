package Spark.Streaming;

import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function0;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
//import java.util.Iterator;

public class SparkStreamingHDFSDemo {
    public static void main(String[] args) throws InterruptedException {
        final SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkStreamingOnHDFS");

//		final String checkpointDirectory = "hdfs://node1:9000/spark/SparkStreaming/CheckPoint2017";
        final String checkpointDirectory = "hdfs://server1:9000/user/spark/checkpoint1";


        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointDirectory, (Function0<JavaStreamingContext>) () -> {
            System.out.println("Creating new context");
            JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));
            ssc.checkpoint(checkpointDirectory);
            JavaReceiverInputDStream<String> lines = ssc.socketTextStream("server2", 9999);
//            JavaDStream<String> lines = ssc.textFileStream("file:///E:/data/pvuvdata");
            JavaDStream<String> words = lines.flatMap(s->Arrays.asList(s.split(" ")).iterator());
            JavaPairDStream<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
            JavaPairDStream<String, Integer> counts =
                            ones.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

                                /**
                                 *
                                 */
                                private static final long serialVersionUID = 1L;

                                @Override
                                public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
                                    /*
                                     * values:经过分组最后 这个key所对应的value  [1,1,1,1,1]
                                     * state:这个key在本次之前之前的状态
                                     */
                                    Integer updateValue = 0 ;
                                    if(state.isPresent()){
                                        updateValue = state.get();
                                    }

                                    for(Integer value : values) {
                                        updateValue += value;
                                    }
                                    return Optional.of(updateValue);
                                }
                            });
//            JavaPairDStream<String, Integer> counts = ones.reduceByKey(Integer::sum);
            System.out.println("*******************");
            counts.print();
            return ssc;
        }
        );
        /*
         * 获取JavaStreamingContext 先去指定的checkpoint目录中去恢复JavaStreamingContext
         * 如果恢复不到，通过factory创建
         */

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

    //	@SuppressWarnings("deprecation")
//    private static JavaStreamingContext createContext(String checkpointDirectory, SparkConf conf) {
//
//        // If you do not see this printed, that means the StreamingContext has
//        // been loaded
//        // from the new checkpoint
//        System.out.println("Creating new context");
//
//        // Create the context with a 1 second batch size
//
//        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));
////		ssc.sparkContext().setLogLevel("WARN");
//        /*
//         *  checkpoint 保存：
//         *		1.配置信息
//         *		2.DStream操作逻辑
//         *		3.job的执行进度
//         *      4.offset
//         */
//        ssc.checkpoint(checkpointDirectory);
//
//        /*
//         * 监控的是HDFS上的一个目录，监控文件数量的变化     文件内容如果追加监控不到。
//         * 只监控文件夹下新增的文件，减少的文件时监控不到的，文件的内容有改动也监控不到。
//         */
////		JavaDStream<String> lines = ssc.textFileStream("hdfs://node1:9000/spark/sparkstreaming");
//        JavaDStream<String> lines = ssc.textFileStream("./data");
//
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
//
//        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Tuple2<String, Integer> call(String s) {
//                return new Tuple2<>(s.trim(), 1);
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
//
//        counts.print();
////		counts.filter(new Function<Tuple2<String,Integer>, Boolean>() {
////
////			/**
////			 *
////			 */
////			private static final long serialVersionUID = 1L;
////
////			@Override
////			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
////				System.out.println("*************************");
////				return true;
////			}
////		}).print();
//        return ssc;
//    }
}
