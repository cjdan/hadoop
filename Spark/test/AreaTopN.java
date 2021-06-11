package Spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class AreaTopN {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("PV");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("file:///E:/data/pvuvdata");
        JavaPairRDD<WebSiteInfo, String> mapToPair = lines.mapToPair(new PairFunction<String, String, Integer>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String line) {

                return new Tuple2<>(line.split("\t")[1] + "_" + line.split("\t")[5], 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) {
                return v1+v2;
            }
        }).mapToPair(new PairFunction<Tuple2<String,Integer>, WebSiteInfo, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<WebSiteInfo, String> call(Tuple2<String,Integer> t) {
                WebSiteInfo webSiteInfo = new WebSiteInfo(t._1.split("_")[1],t._1.split("_")[0],t._2);
                String s = t._1.split("_")[1]+","+t._1.split("_")[0]+"="+t._2;
                return new Tuple2<>(webSiteInfo, s);
            }
        });
        mapToPair.sortByKey().foreach(new VoidFunction<Tuple2<WebSiteInfo,String>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<WebSiteInfo, String> t)     {
                System.out.println(t);
            }
        });
//		List<Tuple2<String, Integer>> take = mapToPair.take(5);
//		for(Tuple2<String, Integer> t:take){
//			System.out.println(t);
//		}
        sc.stop();
    }
}
