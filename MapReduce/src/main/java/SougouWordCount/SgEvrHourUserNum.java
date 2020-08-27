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
import java.util.HashSet;

/**
 * @ClassName: SgEvrHourUserNum
 * @Author: Roohom
 * @Function: 统计每个小时搜狗访问的用户数
 * @Date: 2020/8/27 20:29
 * @Software: IntelliJ IDEA
 */
public class SgEvrHourUserNum extends Configured implements Tool {

    public static class SgMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String userId = value.toString().split("\t")[1];
            String hour = value.toString().substring(0, 2);
            //把小时做为上下文的键，用户id作为值写入上下文
            context.write(new IntWritable(Integer.parseInt(hour)), new Text(userId));
        }
    }

    public static class SgReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        HashSet<String> userSet = new HashSet<>();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                //切记不能直接使用userSet.add(value) 这是一个坑
                //因为直接add实际上是存入迭代器的指针，当迭代器迭代完之后，实际存入set的是迭代器迭代的最后一个值
                userSet.add(value.toString());
            }
            context.write(key, new IntWritable(userSet.size()));
            userSet.clear();
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("D:\\itcast\\LinuxAdvanced&Bigdata\\day08\\05_资料书籍\\sougou_data\\SogouQ.reduced"));

        job.setMapperClass(SgMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SgReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path("datas\\sougouOutput\\HourUserNumOut");
        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new SgEvrHourUserNum(), args);
        System.exit(status);
    }
}
