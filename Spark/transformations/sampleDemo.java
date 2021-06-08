package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class sampleDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("sample");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("hdfs://server1:9000/user/south/input/Test.txt");
        JavaPairRDD<String, Integer> flatMapToPair = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<Tuple2<String, Integer>> call(String t) {
                List<Tuple2<String,Integer>> tupleList = new ArrayList<>();
                tupleList.add(new Tuple2<>(t, 1));
                return tupleList.iterator();
            }
        });
        //withReplacement：表示抽出样本后是否在放回去，true表示会放回去，这也就意味着抽出的样本可能有重.
        //fraction ：抽出多少，这是一个double类型的参数,0-1之间，eg:0.3表示抽出30%
        //seed：表示一个种子，根据这个seed随机抽取，一般情况下只用前两个参数就可以，那么这个参数是干嘛的呢，这个参数一般用于调试，
        // 有时候不知道是程序出问题还是数据出了问题，就可以将这个参数设置为定值
        JavaPairRDD<String, Integer> sampleResult = flatMapToPair.sample(true,0.3,4);
        sampleResult.foreach(t->System.out.println(t._1+t._2));
//        sampleResult.foreach(new VoidFunction<Tuple2<String,Integer>>() {
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
