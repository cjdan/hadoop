package Spark.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class unionDemo {
    public static void main(String[] args) {
        // 设置提交任务的用户
        System.setProperty("HADOOP_USER_NAME", "root");

        // 设置yarn-client模式提交
        // 设置resourcemanager的ip
        // 设置executor的个数
        // 设置executor的内存大小
        // 设置提交任务的yarn队列
        // 设置driver的ip地址
        // 设置jar包的路径,如果有其他的依赖包,可以在这里添加,逗号隔开
        SparkConf conf = new SparkConf()
                .setAppName("unionDemo")
                // 设置yarn-client模式提交
                .setMaster("local");//spark://server1:7077
//                // 设置resourcemanager的ip
//                .set("yarn.resourcemanager.hostname", "server1")
                // 设置executor的个数
//                .set("spark.executor.instance", "2")
//                // 设置executor的内存大小
//                .set("spark.executor.memory", "1024M");
//                // 设置driver的ip地址
//                .set("spark.driver.host", "10.4.101.89");
//        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3),3);
        JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(4,5,6),2);
        JavaRDD<Integer> union = rdd1.union(rdd2);
        List<Integer> res = union.collect();
        System.out.println("res = "+res);
        System.out.println("union.partitions().size()---"+union.partitions().size());
        union.foreach(new VoidFunction<Integer>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Integer t) {
                System.out.println(t);
            }
        });

        sc.stop();
    }
}
