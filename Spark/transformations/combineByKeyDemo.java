package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class combineByKeyDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("combineByKey");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        /*
         * Zhang San 7
         * Li Si 9
         * Wang Wu 5
         */
        JavaPairRDD<String, Integer> parallelizePairs = jsc.parallelizePairs(Arrays.asList(
                new Tuple2<>("Zhang San", 1),
                new Tuple2<>("Zhang San", 2),
                new Tuple2<>("Li Si", 3),
                new Tuple2<>("Zhang San", 4),
                new Tuple2<>("Wang Wu", 5),
                new Tuple2<>("Li Si", 6),
                new Tuple2<>("Li Si", 8)
        ),2);
        parallelizePairs.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String,Integer>>, Iterator<Tuple2<String,Integer>>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<Tuple2<String, Integer>> call(Integer index,
                                                          Iterator<Tuple2<String, Integer>> iter) {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                while(iter.hasNext()){
                    Tuple2<String, Integer> next = iter.next();
                    System.out.println("partitionIndex ="+index+",value="+next);
                    list.add(next);
                }
                return list.iterator();
            }

        }, true).collect();

        System.out.println("****************");
//        combineByKey = parallelizePairs.combineByKey(v1 ->v1, (s,v)->s+v+" ", Integer::sum);
        //
        JavaPairRDD<String, Integer> combineByKey = parallelizePairs.combineByKey(
                new Function<Integer, Integer>() {

                    /**
                     * 把当前分区的第一个值当做v1,这里给每个partition相当于初始化值
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer v1) {
                        System.out.println("~~~~~~~~~~~~~");
                        System.out.println("v1 = " + v1);
                        return v1+1;
                    }
                }, new Function2<Integer, Integer, Integer>() {

                    /**
                     * 合并在同一个partition中的值
                     * call 方法的第一个参数就是分区中的初始值，第二个参数是分区中的第二个值，将结果再赋值给第一个参数，以此类推。。。。。。
                     *
                     * partition 0 :
                     * 	(Zhang San,18)
                     * 	(Zhang San,19)
                     * 	(Li Si,20)
                     *  0分区结果：(Zhang San,18~!19)
                     *  		 (Li Si,20~)
                     * partition 1 :
                     * 	(Zhang San,21)
                     * 	(Wang Wu,22)
                     * 	(Li Si,23)
                     *  1分区结果：(Zhang San,21~)
                     *  		 (Wang Wu,22~)
                     *  		 (Li Si,23~)
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer s, Integer v) {
                        System.out.println("---------");
                        System.out.println("s = " + s+", v = "+v);
                        return s+v;
                    }
                }, new Function2<Integer,Integer,Integer>() {

                    /**
                     * 合并不同partition中的值
                     * partition 0 ：
                     * 	0分区结果：(Zhang San,18~!19)
                     *  		 (Li Si,20~)
                     * partition 1 :
                     *  1分区结果：(Zhang Sa,21~)
                     *  		 (Wang Wu,22~)
                     *  		 (Li Si,23~)
                     *
                     *  分区合并结果
                     *  	(Zhang San,18~!19#21~)
                     *  	(Li Si,20~#23~)
                     *   	(Wang Wu,22~)
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer s1, Integer s2) {
                        System.out.println("===========");
                        System.out.println("s1 = " + s1+", s2 = "+s2);
                        return s1+s2;
                    }
                });
        combineByKey.foreach(new  VoidFunction<Tuple2<String,Integer>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Integer> tuple) {
                System.out.println(tuple);
            }
        });
        jsc.stop();
    }
}
