package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AggregateByKeyDemo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("AggregateByKeyDemo")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<Tuple2<Integer,Integer>> dataList = new ArrayList<>();
        dataList.add(new Tuple2<>(1, 79));
        dataList.add(new Tuple2<>(2, 78));
        dataList.add(new Tuple2<>(1, 89));
        dataList.add(new Tuple2<>(2, 3));
        dataList.add(new Tuple2<>(3, 3));
        dataList.add(new Tuple2<>(3, 30));
        dataList.add(new Tuple2<>(1, 20));

        JavaPairRDD<Integer, Integer> dataRdd = sc.parallelizePairs(dataList,3);
//        dataRdd.mapPartitionsWithIndex()
        JavaPairRDD<Integer, Integer> result = dataRdd.aggregateByKey(80, Math::min, Integer::sum);
        List<Tuple2<Integer, Integer>> res = result.collect();
        System.out.println("res = "+res);
//        dataRdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,Integer>>, Iterator<Tuple2<Integer,Integer>>>() {
//
//            /**
//             *
//             */
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Iterator<Tuple2<Integer, Integer>> call(Integer index,
//                                                           Iterator<Tuple2<Integer, Integer>> iter) {
//                List<Tuple2<Integer, Integer>> list = new ArrayList<>();
//                while(iter.hasNext()){
//                    /**
//                     *
//                     */
//                    System.out.println("partitions --"+index+",value ---"+iter.next());
//                }
//                return list.iterator();
//            }
//        }, true).collect();
//        System.out.println("*****************");
//        //对PairRDD中相同的Key值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。
//        // 和aggregate函数类似，aggregateByKey返回值的类型不需要和RDD中value的类型一致。
//        // 因为aggregateByKey是对相同Key中的值进行聚合操作，所以aggregateByKey函数最终返回的类型还是PairRDD，
//        // 对应的结果是Key和聚合后的值，而aggregate函数直接返回的是非RDD的结果。
//
//        JavaPairRDD<Integer, Integer> aggregateByKey = dataRdd.aggregateByKey(80,
//                new Function2<Integer, Integer, Integer>() {
//                    /**
//                     * 合并在同一个partition中的值，v1的数据类型为zeroValue的数据类型，v2的数据类型为原value的数据类型
//                     * 这里取出来一个分区中相同的key对应的value
//                     *
//                     * 分区0：
//                     * (1,99)
//                     * (1,89)
//                     * (2,78)
//                     *
//                     *
//                     * 分区1：
//                     * (2,3)
//                     * (3,3)
//                     * (3,30)
//                     *
//                     * combiner:
//                     *  0：
//                     *  (1,99)
//                     *  (2,80)
//                     *  1:
//                     *  (2,80)
//                     *  (3,80)
//                     */
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public Integer call(Integer t1, Integer t2) {
//                        System.out.println("seq: " + t1 + "\t " + t2);
//                        return Math.max(t1, t2);
//                    }
//                },new Function2<Integer, Integer, Integer>() {
//
//                    /**
//                     * 合并不同partition中的值，v1，v2的数据类型为zeroValue的数据类型
//                     */
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public Integer call(Integer t1, Integer t2) {
//                        System.out.println("comb: " + t1 + "\t " + t2);
//                        return t1+t2;
//                    }
//
//                });
//        List<Tuple2<Integer,Integer>> resultRdd = aggregateByKey.collect();
//        for (Tuple2<Integer, Integer> tuple2 : resultRdd) {
//            System.out.println("-------------------------");
//            System.out.println(tuple2._1+"\t"+tuple2._2);
//        }
    }
}
