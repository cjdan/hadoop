package Spark.Streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * UpdateStateByKey的主要功能:
 * 1、为Spark Streaming中每一个Key维护一份state状态，state类型可以是任意类型的， 可以是一个自定义的对象，那么更新函数也可以是自定义的。
 * 2、通过更新函数对该key的状态不断更新，对于每个新的batch而言，Spark Streaming会在使用updateStateByKey的时候为已经存在的key进行state的状态更新
 *
 * hello,3
 * Bjsxt,2
 *
 * 如果要不断的更新每个key的state，就一定涉及到了状态的保存和容错，这个时候就需要开启checkpoint机制和功能
 *
 * 全面的广告点击分析
 * @author root
 *
 * 有何用？   统计广告点击流量，统计这一天的车流量，统计点击量
 */


public class UpdateStateByKeyDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyDemo");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        /*
         * 设置checkpoint目录
         *
         * 多久会将内存中的数据（每一个key所对应的状态）写入到磁盘上一份呢？
         * 	如果你的batchInterval小于10s  那么10s会将内存中的数据写入到磁盘一份
         * 	如果bacthInterval 大于10s，那么就以bacthInterval为准
         *
         * 这样做是为了防止频繁的写HDFS
         */

//		JavaSparkContext sc = jsc.sparkContext();
//		sc.setCheckpointDir("./checkpoint");
// 		jsc.checkpoint("hdfs://node1:9000/spark/checkpoint");
        jsc.checkpoint("hdfs://server1:9000/user/spark/checkpoint1");

        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("server2", 9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        words.checkpoint(Duration.apply(10*1000));
        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s, 1);
            }
        });
//        JavaPairDStream<String, Integer> counts = ones.reduceByKey(Integer::sum);
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

        //output operator
        counts.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
