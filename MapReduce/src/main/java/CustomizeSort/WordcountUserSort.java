package CustomizeSort;

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
 * @ClassName: WordcountUserSort
 * @Author: Roohom
 * @Function: 用户自定义排序器来对字段进行降序排序
 * @Date: 2020/8/24 20:28
 * @Software: IntelliJ IDEA
 */
public class WordcountUserSort extends Configured implements Tool {

    public static class myMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split(" ");
            for (String item : items) {
                this.outputKey.set(item);
                context.write(this.outputKey, this.outputValue);
            }
        }
    }

    public static class myReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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


    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "UserSortWordCount");
        job.setJarByClass(WordcountUserSort.class);

        //input
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("datas\\Wordcount\\wordcount.txt"));

        //map
        job.setMapperClass(myMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle
        //TODO:设置自定义排序使用是在Shuffle阶段
        job.setSortComparatorClass(UserSort.class);

        //reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(myReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path("datas\\Wordcount\\UserSortOutput");
        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath))
        {
            hdfs.delete(outputPath,true);
        }
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,outputPath);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new WordcountUserSort(), args);
        System.exit(status);
    }
}
