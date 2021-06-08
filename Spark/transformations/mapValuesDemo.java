package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class mapValuesDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("mapValuesDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, Integer> parallelizePairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 100),
                new Tuple2<>("b", 200),
                new Tuple2<>("c", 300)
        ));
        JavaPairRDD<String, String> mapValues = parallelizePairs.mapValues(new Function<Integer, String>() {//只对value操作

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Integer s) {
                System.out.println("values ------ "+s);
                return s*10+"";
            }
        });
        mapValues.foreach(new VoidFunction<Tuple2<String,String>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, String> arg0) {
                System.out.println(arg0);
            }
        });
        sc.stop();
    }
}
