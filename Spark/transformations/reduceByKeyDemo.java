package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
//import java.util.Iterator;

public class reduceByKeyDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("reduceByKeyDemo");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("hdfs://server1:9000/user/south/input/Test.txt");
        JavaRDD<String> flatMap = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey((Integer::sum));
        reduceByKey.foreach(t->System.out.println("("+t._1+", "+t._2+")"));
//        lines.flatMap(s->Arrays.asList(s.split(" ")).iterator()).mapToPair(s->new Tuple2<>(s,1))
//        .reduceByKey(Integer::sum).foreach(System.out::println);
//        JavaRDD<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {
//
//            /**
//             *
//             */
//            private static final long serialVersionsUID = 1L;
//
//            @Override
//            public Iterator<String> call(String t) {
//                return Arrays.asList(t.split(" ")).iterator();
//            }
//        });
//        JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Tuple2<String, Integer> call(String t) {
//                return new Tuple2<>(t, 1);
//            }
//
//        });
//
//        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer,Integer,Integer>(){
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Integer call(Integer v1, Integer v2) {
//                return v1+v2;
//            }
//
//        },10);
//        reduceByKey.foreach(new VoidFunction<Tuple2<String,Integer>>() {
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public void call(Tuple2<String, Integer> t) {
//                System.out.println(t);
//            }
//        });

        jsc.stop();
    }
}
