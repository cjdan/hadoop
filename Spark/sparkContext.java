package Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class sparkContext {
    public static void main(String[] args) {
        String appName = "sparkContext";
        String master = "local";
        //1.创建一个 SparkContext 对象,每一个 JVM 可能只能激活一个 SparkContext 对象
        /*
            appName 参数是一个在集群 UI 上展示应用程序的名称
            master 是一个 Spark，Mesos 或 YARN 的 cluster URL,或者指定为在 local mode（本地模式）中运行的 “local” 字符串
         */
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        Integer result = distData.map(s -> s * 2).reduce(Integer::sum);

//        Integer result = distData.reduce((l<Integer, Integer, Integer>) Integer::sum);
        System.out.println(result);

    }
}
