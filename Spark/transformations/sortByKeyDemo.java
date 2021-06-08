package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


public class sortByKeyDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("sortByKeyDemo");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("hdfs://server1:9000/user/south/input/Test.txt");
        lines.flatMap(s->Arrays.asList(s.split(" ")).iterator()).mapToPair(s->new Tuple2<>(s, 1))
                //sortByKey：true:降序，false:升序
                //true:降序
                //false:升序
                .reduceByKey(Integer::sum).mapToPair(t->new Tuple2<>(t._2, t._1)).sortByKey(true).mapToPair(t->new Tuple2<>(t._2, t._1))
        .foreach(t->System.out.println(t));
//        JavaRDD<String> flatMap = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//                public Iterator<String> call(String s) {
//                return Arrays.asList(s.split(" ")).iterator();
//            }
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//        });
//        JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Tuple2<String, Integer> call(String s) {
//                return new Tuple2<>(s, 1);
//            }
//        });
//
//        JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
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
//        });
//        reduceByKey.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) {
//                return new Tuple2<>(t._2, t._1);
//            }
//        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) {
//                return new Tuple2<>(t._2, t._1);
//            }
//        }).foreach(new VoidFunction<Tuple2<String,Integer>>() {
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
    }
}
