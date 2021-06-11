package Spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class PipelineDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("PipelineDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1,2,3,4));
        JavaRDD<Integer> map = parallelize.map(new Function<Integer,Integer>(){

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1) throws Exception {
                System.out.println("map-----"+v1);
                return v1;
            }


        });
        JavaRDD<Integer> filter = map.filter(new Function<Integer, Boolean>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(Integer v1) throws Exception {
                System.out.println("filter*********"+v1);
                return true;
            }
        });
        filter.collect();
//        while (true) {

//        }
		sc.stop();
    }
}
