package Spark.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Iterator;

public class foreachPartitionDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("foreachPartitionDemo");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("file:///E:/data/people.csv",3);
        lines.foreachPartition(new VoidFunction<Iterator<String>>() {

            /**
             *
             */
//            private static final long serialVersionUID = -2302401945670821407L;

            @Override
            public void call(Iterator<String> t) throws Exception {
                System.out.println("创建数据库连接。。。");
                while(t.hasNext()){
                    System.out.println(t.next());
                }

            }
        });

        jsc.stop();
    }
}
