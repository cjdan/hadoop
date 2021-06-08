package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

public class subtractDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("subtractDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a","b","c"));
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a","e","f"));
//		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1,2,3));
        //注意使用intersection时，RDD的类型要一致
        JavaRDD<String> intersection = rdd1.subtract(rdd2);//求差集

        intersection.foreach(new VoidFunction<String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(String t) {
                System.out.println(t);
            }
        });
        sc.stop();
    }
}
