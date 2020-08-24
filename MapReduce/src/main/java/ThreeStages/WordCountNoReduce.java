package ThreeStages;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName: WordCountNoReduce
 * @Author: Roohom
 * @Function: MapReduce的三大阶段代码验证 input map output
 * @Date: 2020/8/24 19:54
 * @Software: IntelliJ IDEA
 */
public class WordCountNoReduce extends Configured implements Tool {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split(" ");

            for (String item : items) {
                this.outputKey.set(item);
                context.write(outputKey, outputValue);
            }
        }
    }


    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "countNoReduce");
        job.setJarByClass(WordCountNoReduce.class);

        //input
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("datas\\Wordcount\\wordcount.txt"));
        //Map
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path("datas\\Wordcount\\ThreeStagesOutput");
        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath))
        {
            hdfs.delete(outputPath,true);
        }
        TextOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new WordCountNoReduce(), args);
        System.exit(status);

    }
}
