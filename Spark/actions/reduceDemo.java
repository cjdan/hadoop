package Spark.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

/**
 * reduce
 *
 * 根据聚合逻辑聚合数据集中的每个元素。
 *
 */
public class reduceDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("reduceDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1,2,3,4,5));
//        parallelize.reduce(Integer::sum);

        Integer reduceResult = parallelize.reduce(new Function2<Integer, Integer, Integer>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) {
                return v1+v2;
            }
        });
        System.out.println(reduceResult);
        sc.stop();
    }
}
