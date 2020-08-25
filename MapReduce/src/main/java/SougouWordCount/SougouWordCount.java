package SougouWordCount;

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

/**
 * @ClassName: SougouWordCount
 * @Author: Roohom
 * @Function: 搜狗一天数据的关键词统计，统计每个关键词出现的次数
 * @Date: 2020/8/22 19:15
 * @Software: IntelliJ IDEA
 */
public class SougouWordCount extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "SougouWordCount");
        job.setJarByClass(SougouWordCount.class);

        //input
        Path inputPath = new Path(args[0]);
        TextInputFormat.setInputPaths(job, inputPath);

        //map
        job.setMapperClass(sougouMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle

        //reduce
        job.setReducerClass(sougouReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(args[1]);

        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }
        TextOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : -1;
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.88.221:8020");
        int status = ToolRunner.run(conf, new SougouWordCount(), args);
        System.exit(status);
    }


    public static class sougouMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable(1);

        //重写map方法
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String searchItem = value.toString().split("\t")[2];
            this.outputKey.set(searchItem + "\t");
            context.write(this.outputKey, this.outputValue);
        }
    }


    public static class sougouReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        //定义输出的value
        IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            this.outputValue.set(sum);
            context.write(key, this.outputValue);
        }
    }
}
