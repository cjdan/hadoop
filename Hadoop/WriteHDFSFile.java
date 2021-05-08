package Hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class WriteHDFSFile {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        //1 创建连接

        Configuration conf=new Configuration();
        String filePath = "hdfs://server1:9000/user/south/test/text2.txt";
        URI url = new URI("hdfs://server1:9000");
        String user = "root";
        Path path = new Path(filePath);
        FileSystem fs=FileSystem.get(url,conf,user);



        create(fs,path);
        //2 设置路径
//        String filePath = "hdfs://server1:9000/user/south/test/business.txt";
    }
    //    创建文件夹
    //@Test
    public static void mkdir() throws URISyntaxException, IOException, InterruptedException {
//        1、创建配置文件夹
//        要确认是org.apache.hadoop.conf.Configuration;
        Configuration conf=new Configuration();
        String filePath = "hdfs://server1:9000/user/south/test/text.txt";
        URI url = new URI("hdfs://server1:9000");
        String user = "root";
        Path path = new Path(filePath);
//        2、获取文件系统
        FileSystem fs=FileSystem.get(url,conf,user);
//        3、调用ApI操作
        fs.mkdirs(path);//创建文件
//        4、关闭流
        fs.close();
    }

    //    创建文件并写入数据
    //@Test
    public static void create(FileSystem fs,Path path) throws URISyntaxException, IOException, InterruptedException {
//        1、创建配置文件
        //        要确认是org.apache.hadoop.conf.Configuration;
//        Configuration conf=new Configuration();
//        String filePath = "hdfs://server1:9000/user/south/test/text1.txt";
//        URI url = new URI("hdfs://server1:9000");
//        String user = "root";
//        Path path = new Path(filePath);

//        FileSystem fs=FileSystem.get(url,conf,user);
//        3、调用ApI操作
//        创建文件并写入数据
//        FSDataOutputStream in = fs.append(path);//追加写入
        FSDataOutputStream in=fs.create(path);//创建文件
        for (int i=0;i<10;i++){
            in.write((i+"\n").getBytes());
        }
//        in.write("Hello,everyone".getBytes());
        in.flush();
//       4、关闭流
        fs.close();

    }

    //  读取文件并在控制台展示
    //@Test
    public static void cat() throws URISyntaxException, IOException, InterruptedException {
//        1、创建配置文件
//        要确认是org.apache.hadoop.conf.Configuration;
        Configuration conf=new Configuration();
        String filePath = "hdfs://server1:9000/user/south/test/text1.txt";
        URI url = new URI("hdfs://server1:9000");
        String user = "root";
        Path path = new Path(filePath);
//        2、获取文件系统
        FileSystem fs=FileSystem.get(url,conf,user);
//        3、调用ApI操作
//        创建文件并写入数据
        FSDataInputStream out=fs.open(path);
        IOUtils.copyBytes(out,System.out,1024);
//       4、关闭流
        fs.close();
    }


    //    复制本地文件到hdfs
   // @Test
    public static void copyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
//        1、创建配置文件
        //        要确认是org.apache.hadoop.conf.Configuration;
        Configuration conf=new Configuration();
        String HDFSFilePath = "hdfs://server1:9000/user/south/test";
        String LocalFilePath = "E:\\putty\\putty-64bit-0.74-installer.msi";
        URI url = new URI("hdfs://server1:9000");
        String user = "root";
//        2、获取文件系统
        FileSystem fs=FileSystem.get(url,conf,user);
//        3、调用ApI操作
//   第一个new Path(文件在本机中的路径) ，第二个newPath（hdfs中的路径）
        fs.copyFromLocalFile(new Path(LocalFilePath),new Path(HDFSFilePath));
//       4、关闭流
        fs.close();
    }

    //    复制hdfs文件到本地
   // @Test
    public static void copyTplocalFile() throws URISyntaxException, IOException, InterruptedException {
//        1、创建配置文件
        //        要确认是org.apache.hadoop.conf.Configuration;
        Configuration conf=new Configuration();
        String HDFSFilePath = "hdfs://server1:9000/user/south/test/business.txt";
        String LocalFilePath = "E:\\hadoop\\";
        URI url = new URI("hdfs://server1:9000");
        String user = "root";
//        2、获取文件系统
        FileSystem fs=FileSystem.get(url,conf,user);
//        3、调用ApI操作
//         第一个newPath（hdfs中的路径），第二个new Path(文件在本机中的路径)
//         fs.copyToLocalFile(new Path("/user/java/test.txt"),new Path("C:\\xuexi\\hdfs\\data"));
//         运行如果出现空指针：
//         copyToLocalFile方法第一个参数和最后一个解析：
//          第一个参数控制下载完成后是否删除源文件,默认是 true,即删除;
//          最后一个参数表示是否将 RawLocalFileSystem 用作本地文件系统;
//          RawLocalFileSystem 默认为 false,通常情况下可以不设置,
//          但如果你在执行时候抛出 NullPointerException 异常,则代表你的文件系统与程序可能存在不兼容的情况 (window 下常见),
//          此时可以将 RawLocalFileSystem 设置为 true
        fs.copyToLocalFile(true,new Path(HDFSFilePath),new Path(LocalFilePath),true);
//         4、关闭流
        fs.close();
    }

}
