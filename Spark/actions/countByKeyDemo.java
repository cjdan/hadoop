package Spark.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class countByKeyDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("countByKeyDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<Integer, String> parallelizePairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(1, "a"),
                new Tuple2<>(2, "b"),
                new Tuple2<>(3, "c"),
                new Tuple2<>(4, "d"),
                new Tuple2<>(4, "e")
        ));

        Map<Integer, Long> countByKey = parallelizePairs.countByKey();
        for(Map.Entry<Integer,Long> entry : countByKey.entrySet()){
            System.out.println("key:"+entry.getKey()+", value:"+entry.getValue());
        }
    }
}
