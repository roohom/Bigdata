package WordCount;

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
 * @ClassName: WordCount.SimpleMethod.MRWordCount
 * @Author: Roohom
 * @Function: 自定义开发实现WordCount，使用combiner
 * @Date: 2020/8/22 15:38
 * @Software: IntelliJ IDEA
 */
public class MRWordCountLocal extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "userCount");
        job.setJarByClass(MRWordCountLocal.class);

        Path inputPath = new Path("/wordcount/input/wordcount.txt");
        TextInputFormat.setInputPaths(job, inputPath);

        //map
        //设置mapper类是谁
        job.setMapperClass(WcMapper.class);
        //指定map输出的key类型，也就是K2的类型
        job.setMapOutputKeyClass(Text.class);
        //指定map输出的value类型，也就是V2的类型
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle
        //使用combiner
        job.setCombinerClass(WcReducer.class);

        //reduce
        job.setReducerClass(WcReducer.class);
        //设置reduce的输出key的类型，也就是K3的类型
        job.setOutputKeyClass(Text.class);
        //设置reduce的输出value的类型，也就是V3的类型
        job.setOutputValueClass(IntWritable.class);
        //调节reduce的个数，默认为1
        job.setNumReduceTasks(1);

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path("/wordcount/output2");

        //判断输出路径是否已存在(使用hdfs的JAVA API)
        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        TextOutputFormat.setOutputPath(job, outputPath);

        //提交运行
        //三目运算
        return job.waitForCompletion(true) ? 0 : -1;
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defultFS", "hdfs://192.168.88.221:8020");

        int status = ToolRunner.run(conf, new MRWordCountLocal(), args);
        System.exit(status);
    }


    public static class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable ouputValue = new IntWritable(1);

        /**
         * Input 传递过来的每个KV调用一次map
         *
         * @param key     偏移量
         * @param value   本行内容
         * @param context 上下文
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");

            //取出每个单词
            for (String word : words) {
                //将单词作为新的key，value恒为1
                this.outputKey.set(word);
                context.write(this.outputKey, this.ouputValue);
            }
        }
    }


    public static class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        //定义输出的value
        IntWritable outputValue = new IntWritable();

        /**
         * shuffle分组排序以后的数据会进入reduce每一组数据/每种key会调用一次reduce
         *
         * @param key     K2
         * @param values  相同的K2对应的所有value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //定义求和
            int sum = 0;
            //迭代 取出当前key对应的所有value
            for (IntWritable value : values) {
                //累加
                sum += value.get();
            }
            //将累加的结果作为输出的value
            this.outputValue.set(sum);
            context.write(key, this.outputValue);
        }
    }
}
