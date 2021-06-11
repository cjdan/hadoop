package Spark.test;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 自定义分区器
 *
 */
public class PartitionerDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("partitioner");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(1, "Zhang San"),
                new Tuple2<>(2, "Li Si"),
                new Tuple2<>(3, "Wang Wu"),
                new Tuple2<>(4, "Zhao Liu"),
                new Tuple2<>(5, "Shun Qi"),
                new Tuple2<>(6, "Zhou Ba")
        ), 2);

        nameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,String>>, Iterator<String>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(Integer index, Iterator<Tuple2<Integer, String>> iter) {
                List<String> list = new ArrayList<>();
                while(iter.hasNext()){
                    System.out.println("nameRDD partitionID = "+index+" , value = "+iter.next());
                }
                return list.iterator();
            }
        }, true).collect();
        System.out.println("******************************");
//        nameRDD.partitionBy(2,key->)
        JavaPairRDD<Integer, String> partitionRDD = nameRDD.partitionBy(new Partitioner() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            /*
             * 返回你想要创建分区的个数
             */
            public int numPartitions() {
                return 3;
            }

            @Override
            /*
             * 对输入的key做计算，然后返回该key的分区ID，范围一定是0到numPartitions-1
             */
            public int getPartition(Object key) {
                int i = (int)key;
                return i%3;
            }
        });
        partitionRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer,String>>, Iterator<String>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(Integer index, Iterator<Tuple2<Integer, String>> iter) {
                List<String> list = new ArrayList<>();
                while(iter.hasNext()){
                    System.out.println("partitionRDD partitionID = "+index+" , value = "+iter.next());
                }
                return list.iterator();
            }
        }, true).collect();

        sc.stop();
    }
}
