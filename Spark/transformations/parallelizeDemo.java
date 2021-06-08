package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class parallelizeDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("parallelizeDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList(
                "love1","love2","love3",
                "love4","love5","love6",
                "love7","love8","love9",
                "love10","love11","love12",
                "love13","love14","love15",
                "love13","love14","love15",
                "love13","love14","love15",
                "love13","love14","love15",
                "love13","love14","love15",
                "love13","love14","love15",
                "love13","love14","love15",
                "love13","love14","love15",
                "love13","love14","love15",
                "love13","love14","love15",
                "love13","love14","love15"
        );
        System.out.println(list);
        JavaRDD<String> rdd1 = sc.parallelize(list,3);
        System.out.println(rdd1.partitions().size());
        List<String> result = rdd1.collect();
        System.out.println(result);
    }
}
