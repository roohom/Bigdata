package MapReduceCodeModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
 * @ClassName: MRModelTotal
 * @Author: Roohom
 * @Function: MapReduce编程模板
 * @Date: 2020/8/22 18:14
 * @Software: IntelliJ IDEA
 */
public class MRModelTotal extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "MRModel");
        job.setJarByClass(MRModelTotal.class);
        //input
        job.setInputFormatClass(TextInputFormat.class);
        Path inputPath = new Path(args[0]);
        TextInputFormat.setInputPaths(job, inputPath);

        //map
        job.setMapperClass(null);
        job.setMapOutputKeyClass(null);
        job.setMapOutputValueClass(null);

        //shuffle

        //reduce
        job.setReducerClass(null);
        job.setOutputKeyClass(null);
        job.setOutputValueClass(null);
        job.setNumReduceTasks(1);


        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(args[1]);
        TextOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new MRModelTotal(), args);
        System.exit(status);
    }

    public class MRModelMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        /**
         * 所有的input传递的KV都会调用一次map方法
         *
         * @param key     当前的K1
         * @param value   当前的V1
         * @param context 上下文对象，用于获取数据，向下传递数据
         * @throws IOException          IO异常
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //自定义处理数据
        }
    }

    public class MRModelReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        /**
         * shuffle以后每组的会调用一次reduce方法
         *
         * @param key     K2
         * @param values  相同的K2的所有V2放入一个迭代器中
         * @param context 上下文对象，用于传递数据
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //处理逻辑由需求来定义
        }
    }
}
