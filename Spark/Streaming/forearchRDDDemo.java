package Spark.Streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class forearchRDDDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("forearchRDDDemo");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaDStream<String> textFileStream = jsc.textFileStream("file:///E:/data/data1/");

            textFileStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            public void call(JavaRDD<String> t) {
                t.foreach(new VoidFunction<String>() {

                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    public void call(String t) {
                        System.out.println("**********************");
                        System.out.println("t = "+t);

                    }
                });
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
