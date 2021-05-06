package Hadoop.MapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable valueout = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int count = 0; // 统计总数

        // 遍历数组，累加求和
        for(IntWritable value : values){

            // IntWritable类型不能和int类型相加，所以需要先使用get方法转换成int类型
            count += value.get();
        }

        // 将统计的结果转成IntWritable
        valueout.set(count);

        // 最后reduce要输出最终的 k v 对
        context.write(key, valueout);

    }

    
}