package Hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DeleteHDFSFile {
    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        Configuration conf=new Configuration();//加载配置
        String OutputFilePath = "/user/south/input/test2.txt";
        String OutputFilePath1 = "/user/south/input";
        Path path = new Path(OutputFilePath);
        Path path1 = new Path(OutputFilePath1);
        URI url = new URI("hdfs://server1:9000");
        FileSystem fs = FileSystem.get(url,conf,"root");
        del(path,path1,fs);

    }
    public static void del(Path path,Path path1,FileSystem fs) throws IOException {
        //删除文件（夹）

        //加载文件系统实例，需要填写
        FileStatus[] liststatus=fs.listStatus(path1);//无递归
        for(FileStatus status: liststatus){
            System.out.println("删除前");
            System.out.println("文件路径"+status.getPath());
            System.out.println(status.isDirectory()?"这是文件夹":"这是文件");
            System.out.println(status.getReplication());

        }
        fs.delete(path,true);//删除路径
        FileStatus[] liststatus1=fs.listStatus(path1);//无递归
        for(FileStatus status: liststatus1){
            System.out.println("删除后");
            System.out.println("文件路径"+status.getPath());
            System.out.println(status.isDirectory()?"这是文件夹":"这是文件");
            System.out.println(status.getReplication());

        }




        fs.close();
    }

}
