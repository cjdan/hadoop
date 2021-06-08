package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class filterDemo {
    public static void main(String[] args) {
        /**
         * SparkConf对象中主要设置Spark运行的环境参数。
         * 1.运行模式
         * 2.设置Application name
         * 3.运行的资源需求
         */
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("filterDemo");
        /**
         * JavaSparkContext对象是spark运行的上下文，是通往集群的唯一通道。
         */
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("hdfs://server1:9000/user/south/input/Test.txt");
        lines.filter(s->!s.contains("m")).foreach(t->System.out.println(t.contains("hello")));
//        JavaRDD<String> resultRDD = lines.filter(new Function<String, Boolean>() {
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Boolean call(String line) {
//                return !line.contains("m");
//            }
//
//        });
//
//        resultRDD.foreach(new VoidFunction<String>() {
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public void call(String line) throws Exception {
//                System.out.println(line);
//            }
//        });
        jsc.stop();
    }
}
