package Spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/*
*
*
*/

public class CacheDemo {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("CacheDemo");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = jsc.textFile("file:///E:/data/pvuvdata");
//        System.out.println("level = "+rdd1.getStorageLevel().description() + ", "+rdd1);
//        rdd1 = rdd1.cache();
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(x -> new Tuple2<>(x, 1));
        JavaRDD<String> rdd3 = rdd1.filter(x -> x.contains("www.baidu.com"));
        rdd2.cache().setName("rdd2");
        rdd3.cache().setName("rdd3");
//        rdd2.unpersist();
        Map<Integer, JavaRDD<?>> rdds = jsc.getPersistentRDDs();
        Set<Integer> key = rdds.keySet();
        System.out.println("key = "+ key);
        JavaRDD<?> rdd_2 = rdds.get(2);
        rdd_2.foreach(x->System.out.println(x));

        System.out.println("rdds = "+rdds);


//
//        long startTime = System.currentTimeMillis();
//        long rdd4 = rdd2.count();
//        long endTime = System.currentTimeMillis();
//        System.out.println("rdd4 = "+rdd4+":"+(endTime-startTime));
//        rdd1.unpersist();//释放
//        long startTime1 = System.currentTimeMillis();
//        long rdd5 = rdd3.count();
//        long endTime1 = System.currentTimeMillis();
//        System.out.println("rdd4 = "+rdd5+":"+(endTime1-startTime1));
//        rdd1 = rdd1.cache();
//		lines = lines.persist(StorageLevel.MEMORY_ONLY_2());
//        lines = lines.persist(StorageLevel.MEMORY_AND_DISK());
//        long startTime = System.currentTimeMillis();
//        long count = lines.count();
//        long endTime = System.currentTimeMillis();
//        System.out.println("共"+count+ "条数据，"+"初始化时间+cache时间+计算时间="+ (endTime-startTime));
//		long countStartTime = System.currentTimeMillis();
//        JavaRDD<String> res = lines.filter(x -> x.contains("matlab"));
//		long countrResult = lines.count();
//		long countEndTime = System.currentTimeMillis();
//		System.out.println("共"+countrResult+ "条数据，"+"计算时间="+ (countEndTime-countStartTime));
//		lines.unpersist();//action之后unpersisit，大幅度提高运行效率
//        lines.unpersist();
//		long countStartTime2 = System.currentTimeMillis();
//		long countrResult2 = lines.count();
//		long countEndTime2 = System.currentTimeMillis();
//		System.out.println("共"+countrResult2+ "条数据，"+"计算时间="+ (countEndTime2-countStartTime2));
//        long countStartTime3 = System.currentTimeMillis();
//        long countrResult3 = lines1.count();
//        long countEndTime3 = System.currentTimeMillis();
//        System.out.println("共"+countrResult3+ "条数据，"+"计算时间="+ (countEndTime3-countStartTime3));
//        lines1.unpersist();
//        long countStartTime4 = System.currentTimeMillis();
//        long countrResult4 = lines1.count();
//        long countEndTime4 = System.currentTimeMillis();
//        System.out.println("共"+countrResult4+ "条数据，"+"计算时间="+ (countEndTime4-countStartTime4));


//		lines.unpersist();
//        while(true){
//
//        }

		jsc.stop();
    }
}
