//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package Hadoop.MapReduce;

import com.google.common.base.Charsets;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordMean extends Configured implements Tool {
    private double mean = 0.0D;
    private static final Text COUNT = new Text("count");
    private static final Text LENGTH = new Text("length");
    private static final LongWritable ONE = new LongWritable(1L);

    public WordMean() {
    }

    private double readAndCalcMean(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(path, "part-r-00000");
        if (!fs.exists(file)) {
            throw new IOException("Output not found!");
        } else {
            BufferedReader br = null;

            double var19;
            try {
                InputStreamReader inputStreamReader = new InputStreamReader(fs.open(file), Charsets.UTF_8);
//                char[] chs = new char[1024];
//                int len = 0;
//                while ((len = inputStreamReader.read(chs)) != -1) {
//                    System.out.print(new String(chs, 0, len));
//                }//打印inputStreamReader内容

                br = new BufferedReader(inputStreamReader);
                System.out.println("br = "+br.read());
                long count = 0L;
                long length = 0L;

                String line;
                while((line = br.readLine()) != null) {
                    System.out.println("line = "+line);
                    StringTokenizer st = new StringTokenizer(line,"\t");
                    String type = st.nextToken();
                    String lengthLit;
                    System.out.println("type = "+type);
                    if (type.equals(COUNT.toString())) {
                        lengthLit = st.nextToken();
                        System.out.println("lengthLit = "+lengthLit);
                        count = Long.parseLong(lengthLit);
                    } else if (type.equals(LENGTH.toString())) {
                        lengthLit = st.nextToken();
                        System.out.println("lengthLit = "+lengthLit);
                        length = Long.parseLong(lengthLit);
                    }
                }

                double theMean = (double)length / (double)count;
                System.out.println("The mean is: " + theMean);
                var19 = theMean;
            } finally {
                if (br != null) {
                    br.close();
                }

            }

            return var19;
        }
    }

    public static void main(String[] args) throws Exception {
        String InputPath = "/user/south/input/Test.txt";
        String OutputtPath = "/user/south/input/test.txt";
        args = new String[]{InputPath,OutputtPath};
        ToolRunner.run(new Configuration(), new WordMean(), args);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: wordmean <in> <out>");
            return 0;
        } else {
            Configuration conf = this.getConf();
            Job job = Job.getInstance(conf, "word mean");
            job.setJarByClass(WordMean.class);
            job.setMapperClass(WordMean.WordMeanMapper.class);
            job.setCombinerClass(WordMean.WordMeanReducer.class);
            job.setReducerClass(WordMean.WordMeanReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            Path outputpath = new Path(args[1]);
            FileOutputFormat.setOutputPath(job, outputpath);
            boolean result = job.waitForCompletion(true);
            this.mean = this.readAndCalcMean(outputpath, conf);
            return result ? 0 : 1;
        }
    }

    public double getMean() {
        return this.mean;
    }

    public static class WordMeanReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable sum = new LongWritable();

        public WordMeanReducer() {
        }

        public void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            int theSum = 0;

            LongWritable val;
            for(Iterator i$ = values.iterator(); i$.hasNext(); theSum = (int)((long)theSum + val.get())) {
                val = (LongWritable)i$.next();
            }

            this.sum.set((long)theSum);
            context.write(key, this.sum);
        }
    }

    public static class WordMeanMapper extends Mapper<Object, Text, Text, LongWritable> {
        private LongWritable wordLen = new LongWritable();

        public WordMeanMapper() {
        }

        public void map(Object key, Text value, Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while(itr.hasMoreTokens()) {
                String string = itr.nextToken();
                this.wordLen.set((long)string.length());
                context.write(WordMean.LENGTH, this.wordLen);
                context.write(WordMean.COUNT, WordMean.ONE);
            }

        }
    }
}
