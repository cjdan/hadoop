package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * repartition
 * 减少或者增多分区，会产生shuffle.(多个分区分到一个分区中不会产生shuffle)
 *使用repartition 使得任务能够并行执行的话，分配的core的数量一定要略微大于最大的分区数，才能使得所有的task能够并行执行。
 */
public class repartitionDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("repartitionDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList(
                "love1","love2","love3",
                "love4","love5","love6",
                "love7","love8","love9",
                "love10","love11","love12"
        );

        JavaRDD<String> rdd1 = sc.parallelize(list,3);
        JavaRDD<String> rdd2 = rdd1.mapPartitionsWithIndex(
                new Function2<Integer, Iterator<String>, Iterator<String>>(){

                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<String> call(Integer partitionId, Iterator<String> iter) {
                        List<String> list = new ArrayList<>();
                        System.out.println("shuffle前==========================");
                        while(iter.hasNext()){
                            list.add("RDD1的分区索引:【"+partitionId+"】,值为："+iter.next());
                        }
                        return list.iterator();
                    }

                }, true);
//        JavaRDD<String> repartitionRDD = rdd2.repartition(1);
//		JavaRDD<String> repartitionRDD = rdd2.repartition(2);
		JavaRDD<String> repartitionRDD = rdd2.repartition(12);
        JavaRDD<String> result = repartitionRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>(){

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(Integer partitionId, Iterator<String> iter) {
                List<String> list = new ArrayList<>();
                while(iter.hasNext()){
                    list.add("repartitionRDD的分区索引:【"+partitionId+"】,值为：	"+iter.next());

                }
                return list.iterator();
            }

        }, true);
        System.out.println("shuffle后==========================");
        for(String s: result.collect()){
            System.out.println(s);
        }
        sc.stop();

    }
}
