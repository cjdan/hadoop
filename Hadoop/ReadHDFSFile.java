package Hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ReadHDFSFile {
    public static void main(String[] args) throws IOException {
        //1 创建连接
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        //2 设置路径
        String filePath = "hdfs://server1:9000/user/south/test/business.txt";
        //3 定义输入流
        FSDataInputStream fsDataInputStream = null;
        String line;
        try {
            //4 转换为hdfs识别的文件路径，String转Path
            Path path = new Path(filePath);
            System.out.println("path = "+path);
            //5 读取文件,转化为流
            fsDataInputStream = fileSystem.open(path);
            //5.1 一般按照格式读取
            InputStreamReader isr = new InputStreamReader(fsDataInputStream, "utf-8");
            System.out.println("isr = "+isr);
            BufferedReader br = new BufferedReader(isr);
            System.out.println("br = "+br);
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            //5.2 copy读取
//            IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);//打印文件内容

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(fsDataInputStream != null){
                IOUtils.closeStream(fsDataInputStream);
            }
        }

    }

}
