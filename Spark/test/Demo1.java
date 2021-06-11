package Spark.test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
/**
 * 动态统计出现次数最多的单词个数，过滤掉。
 * @author root
 *
 */
public class Demo1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("demo1");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("file:///E:/data/NASA_access_log_Aug95");
        JavaRDD<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String t) {
                return Arrays.asList(t.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String,String, Integer>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String t) {
                return new Tuple2<>(t, 1);
            }
        });

        JavaPairRDD<String, Integer> sample = mapToPair.sample(true, 0.5);

        final List<Tuple2<String, Integer>> take = sample.reduceByKey(new Function2<Integer,Integer,Integer>(){

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) {
                return v1+v2;
            }

        }).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) {
                return new Tuple2<>(t._2, t._1);
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) {
                return new Tuple2<>(t._2, t._1);
            }
        }).take(1);

        System.out.println("take--------"+take);

        JavaPairRDD<String, Integer> result = mapToPair.filter(new Function<Tuple2<String,Integer>, Boolean>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(Tuple2<String, Integer> v1) {
                return !v1._1.equals(take.get(0)._1);
            }
        }).reduceByKey(new Function2<Integer,Integer,Integer>(){

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) {
                return v1+v2;
            }

        }).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) {
                return new Tuple2<>(t._2, t._1);
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) {
                return new Tuple2<>(t._2, t._1);
            }
        });

        result.foreach(new VoidFunction<Tuple2<String,Integer>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Integer> t) {
                System.out.println(t);
            }
        });

        jsc.stop();

    }
}
