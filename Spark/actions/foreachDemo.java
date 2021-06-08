package Spark.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

public class foreachDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("foreachDemo");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<Integer> parallelize = jsc.parallelize(Arrays.asList(1,2,3));

        parallelize.foreach(new VoidFunction<Integer>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Integer t) {
                System.out.println(t);
            }
        });

        jsc.stop();
    }
}
