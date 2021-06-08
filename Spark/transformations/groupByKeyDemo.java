package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

public class groupByKeyDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("groupByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, Integer> parallelizePairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 2),
                new Tuple2<>("b", 3),
                new Tuple2<>("c", 4),
                new Tuple2<>("d", 5),
                new Tuple2<>("d", 6),
                new Tuple2<>("d", 7),
                new Tuple2<>("d", 8)
        ),3);

        JavaPairRDD<String, Iterable<Integer>> groupByKey = parallelizePairs.groupByKey(1);
//        JavaRDD<Tuple2<String, Integer>> res = groupByKey.map(new Function<Tuple2<String, Iterable<Integer>>, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> v1) throws Exception {
//                Iterable<Integer> res = v1._2;
//                int sum = 0;
//
//                for (Integer next : res) {
//                    sum+=next;
////                    System.out.println("next = " + next);
//                }
//                return new Tuple2<>(v1._1, sum);
//            }
////        });
//        List<Tuple2<String, Integer>> result = res.collect();
//        System.out.println("result = "+result);
//        res.foreach((VoidFunction<Tuple2<String, Integer>>) System.out::println);
        int partition_num = groupByKey.partitions().size();
        JavaRDD<List<Tuple2<String, Iterable<Integer>>>> res = groupByKey.glom();//glom的作用是将同一个分区里的元素合并到一个array里
        res.foreach(t->System.out.println("t="+t));
        System.out.println(partition_num);

//        groupByKey.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Iterable<Integer>>>, Iterator<? extends Object>>() {
//            @Override
//            public Iterator<? extends Object> call(Integer v1, Iterator<Tuple2<String, Iterable<Integer>>> v2) throws Exception {
//                return null;
//            }
//        },true);
//        JavaRDD<String> mapPartitionsWithIndex = groupByKey.mapPartitionsWithIndex(
//                new Function2<Integer, Iterator<String>, Iterator<String>>() {
//                    /*
//                     *
//                     */
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public Iterator<String> call(Integer index, Iterator<String> iter) {
//                        List<String> list = new ArrayList<>();
//                        while(iter.hasNext()){
//                            String s = iter.next();
//                            list.add(s+"~");
//                            System.out.println("partition id is "+index +",value is "+s );
//                        }
//                        return list.iterator();
//                    }
//                }, true);
//        mapPartitionsWithIndex.collect();

        groupByKey.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) {


                System.out.println(t);
            }
        });
    }
}
