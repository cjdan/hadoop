package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class mapPartitionWithIndexDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("mapPartitionWithIndexDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> names = Arrays.asList("Zhang San", "Li Si", "Wang Wu","jack");

        /*
         * 这里的第二个参数是设置并行度,也是RDD的分区数，并行度理论上来说设置大小为core的2~3倍
         */
        JavaRDD<String> parallelize = sc.parallelize(names, 3);
        //类似于mapPartitions,除此之外还会携带分区的索引值。
        //preservesPartitioning:
        //RDD中的所有数据通过JDBC连接写入数据库，如果使用map函数，可能要为每一个元素都创建一个connection，这样开销很大，
        // 如果使用mapPartitions，那么只需要针对每一个分区建立一个connection。
        //参数preservesPartitioning表示是否保留父RDD的partitioner分区信息。
        System.out.println(parallelize.partitions().size());
        JavaRDD<String> mapPartitionsWithIndex = parallelize.mapPartitionsWithIndex(
                new Function2<Integer, Iterator<String>, Iterator<String>>() {
                    /*
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<String> call(Integer index, Iterator<String> iter) {
                        List<String> list = new ArrayList<>();
                        while(iter.hasNext()){
                            String s = iter.next();
                            list.add(s+"~");
                            System.out.println("partition id is "+index +",value is "+s );
                        }
                        return list.iterator();
                    }
                }, true);
        mapPartitionsWithIndex.collect();
        sc.stop();
    }
}
