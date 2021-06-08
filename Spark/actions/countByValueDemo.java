package Spark.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class countByValueDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("countByValueDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<Integer, String> parallelizePairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(1, "a"),
                new Tuple2<>(2, "b"),
                new Tuple2<>(2, "c"),
                new Tuple2<>(3, "c"),
                new Tuple2<>(4, "d"),
                new Tuple2<>(4, "d")
        ));
        //根据rdd中的元素值相同的个数。返回的类型为Map[K,V],  K : 元素的值，V ：元素对应的的个数
        Map<Tuple2<Integer, String>, Long> countByValue = parallelizePairs.countByValue();

        for(Map.Entry<Tuple2<Integer, String>, Long> entry : countByValue.entrySet()){
            System.out.println("key:"+entry.getKey()+", value:"+entry.getValue());
        }
    }
}
