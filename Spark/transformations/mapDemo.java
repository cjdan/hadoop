package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class mapDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("map");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> line = jsc.textFile("file:///E:/data/people.csv");
        JavaRDD<Map<String,Integer>> mapResult = line.map(new Function<String,  Map<String,Integer>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Map<String,Integer> call(String s) throws Exception {
                HashMap<String, Integer> map = new HashMap<String, Integer>();
                String s1 = s.split(",")[1];
                map.put(s1,1);
                return map;
            }
        });


        mapResult.foreach(new VoidFunction<Map<String,Integer>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Map<String,Integer> map) throws Exception {

                for(Map.Entry<String, Integer> entry : map.entrySet()){
                    System.out.println("key = " + entry.getKey() + ", value = " + entry.getValue());
                }
            }
        });
        jsc.stop();
    }
}

