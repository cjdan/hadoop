package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class mapPartitionDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("mapPartitionDemo");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("hdfs://server1:9000/user/south/test/text1.txt," +
                "hdfs://server1:9000/user/south/test/text2.txt,hdfs://server1:9000/user/south/input/Test.txt",1);
        //minPartitions:结果存入几个partition

//		lines.map(new Function<String, String>() {
//
//			/**
//			 *
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String call(String arg0) throws Exception {
//				System.out.println("创建数据库链接对象。。。。");
//				System.out.println("插入数据库。。。"+arg0);
//				System.out.println("关闭数据库链接。。。");
//				return arg0;
//			}
//		}).collect();
//

        JavaRDD<String> mapPartitions = lines.mapPartitions((FlatMapFunction<Iterator<String>, String>) stringIterator -> {
            List<String> list=new ArrayList<>();
            while (stringIterator.hasNext()){
                String next = stringIterator.next();
                if(next.contains("e")){
                    list.add(stringIterator.next());
                }

            }
            return list.iterator();
        },true);

        List<String> res = mapPartitions.collect();
        System.out.println("长度为"+res.size());
        for (String re : res) {
            System.out.println(re);
        }
//        System.out.println(res);

        sc.stop();
    }
}
