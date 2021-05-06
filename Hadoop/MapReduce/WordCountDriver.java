package Hadoop.MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        String InputFilePath = "/user/south/input/test.txt";
        String OutputFilePath = "/user/south/input/test1.txt";
        Path InputPath = new Path(InputFilePath);
        Path OutputPath = new Path(OutputFilePath);

        // 获得一个job对象，用来完成一个mapreduce作业
        Job job = Job.getInstance(conf);


        // 让程序找到主入口
        job.setJarByClass(WordCountDriver.class);
        //设置Mapper组件类
        // 指定输入数据的目录，指定数据计算完成后输出的目录
        // sbin/yarn jar share/hadoop/xxxxxxx.jar wordcount /wordcount/input/ /wordcount/output/
//        FileInputFormat.addInputPath(job, InputPath);//对应org.apache.hadoop.mapreduce.lib.input.FileInputFormat库
//        FileOutputFormat.setOutputPath(job, OutputPath);//对应org.apache.hadoop.mapreduce.lib.input.FileInputFormat库

        // 告诉我调用那个map方法和reduce方法
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 为Job创建一个名字
        job.setJobName("wordcount");

        // 指定map输出键值对的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定reduce输出键值对的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,InputPath);
        FileOutputFormat.setOutputPath(job,OutputPath);


        // 提交job任务
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
