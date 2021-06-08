package Spark.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;

/**
 * collect
 * 将计算的结果作为集合拉回到driver端，一般在使用过滤算子或者一些能返回少量数据集的算子后，将结果回收到Driver端打印显示。
 *
 */
public class collectDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("collectDemo");
        /*
         * JavaSparkContext对象是spark运行的上下文，是通往集群的唯一通道。
         */
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("file:///E:/data/people.csv");
        JavaRDD<String> resultRDD = lines.filter(s -> !s.contains("female"));
//        JavaRDD<String> resultRDD = lines.filter(new Function<String, Boolean>() {
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Boolean call(String line) {
//                return !line.contains("female");
//            }
//
//        });
        List<String> collect = resultRDD.collect();
        for(String s :collect){
            System.out.println(s);
        }

        jsc.stop();
    }
}
