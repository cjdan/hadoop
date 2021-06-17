package Spark.Streaming;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

public class CopyFile_data {
    public static void main(String[] args) throws IOException, InterruptedException {
        while(true){
            Thread.sleep(5000);
            String uuid = UUID.randomUUID().toString();
            System.out.println(uuid);
            copyFile(new File("E:/data/test1.txt"),new File("E:/data/data1/"+uuid+"----data.txt"));
        }
    }

    public static void copyFile(File fromFile, File toFile) throws IOException {
        FileInputStream ins = new FileInputStream(fromFile);
        FileOutputStream out = new FileOutputStream(toFile);
        byte[] b = new byte[1024*1024];
        @SuppressWarnings("unused")
        int n = 0;
        while ((n = ins.read(b)) != -1) {
            out.write(b, 0, b.length);
        }

        ins.close();
        out.close();
    }
}
