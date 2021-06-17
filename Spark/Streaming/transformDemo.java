package Spark.Streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;


import java.util.Collections;
import java.util.List;

/*
* transform操作
* DStream可以通过transform做RDD到RDD的任意操作。
* @author root
*/

public class transformDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]").setAppName("transformDemo");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        //黑名单
        List<String> list = Collections.singletonList("ZhangSan");
        final Broadcast<List<String>> bcBlackList = jsc.sparkContext().broadcast(list);

        //接受socket数据源
        JavaReceiverInputDStream<String> nameList = jsc.socketTextStream("server2", 9999);
        JavaPairDStream<String, String> pairNameList =
                nameList.mapToPair(new PairFunction<String, String, String>() {

                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(String s) {

                        return new Tuple2<>(s.split(" ")[1], s);
                    }
                });
        /*
         * transform 可以拿到DStream中的RDD，做RDD到RDD之间的转换，不需要Action算子触发，需要返回RDD类型。
         * 注意：transform call方法内，拿到RDD 算子外的代码 在Driver端执行，也可以做到动态改变广播变量。
         */
        JavaDStream<String> transFormResult =
                pairNameList.transform(new Function<JavaPairRDD<String,String>, JavaRDD<String>>() {

                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JavaRDD<String> call(JavaPairRDD<String, String> nameRDD) {

                        JavaPairRDD<String, String> filter = nameRDD.filter(new Function<Tuple2<String,String>, Boolean>() {

                            /**
                             *
                             */
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Boolean call(Tuple2<String, String> tuple) {
                                System.out.println("tuple = " + tuple);
                                return !bcBlackList.value().contains(tuple._1);
                            }
                        });

                        //返回过滤好的结果
                        /*
                         *
                         */
                        return filter.map((Function<Tuple2<String, String>, String>) tuple -> tuple._2);
                    }
                });

        transFormResult.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();

    }
}
