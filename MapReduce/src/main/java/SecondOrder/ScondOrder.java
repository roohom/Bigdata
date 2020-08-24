package SecondOrder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @ClassName: ScondOrder
 * @Author: Roohom
 * @Function: 二次排序，使用list集合方式，如果在数据量巨大时会对内存和CPU造成过大负载,
 * @Date: 2020/8/24 21:21
 * @Software: IntelliJ IDEA
 */
public class ScondOrder extends Configured implements Tool {
    public static class myMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split(" ");
            outputKey.set(items[0]);
            //字符本身作为键
            int anInt = Integer.parseInt(items[1]);
            //数字作为值
            outputValue.set(anInt);
            context.write(outputKey,outputValue);
        }
    }

    //关键在于reduce阶段对值进行排序，map阶段已经对键排序了
    public static class myReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<Integer> itemList = new ArrayList<Integer>();
            for (IntWritable value : values) {
                itemList.add(value.get());
            }
            Collections.sort(itemList);
            for (Integer integer : itemList) {
                context.write(key, new IntWritable(integer));
            }
        }
    }


    public int run(String[] agrs) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Seconder");
        job.setJarByClass(ScondOrder.class);

        //input
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("datas\\Order\\text.txt"));

        //map
        job.setMapperClass(myMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reduce
        job.setReducerClass(myReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path("datas\\Order\\output");
        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }
        TextOutputFormat.setOutputPath(job,outputPath);
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new ScondOrder(), args);
        System.exit(status);
    }
}
