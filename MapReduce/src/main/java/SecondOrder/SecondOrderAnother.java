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

/**
 * @ClassName: SecondOrderAnother
 * @Author: Roohom
 * @Function: 正经的MapReduce二次排序
 * @Date: 2020/8/24 23:07
 * @Software: IntelliJ IDEA
 */
public class SecondOrderAnother extends Configured implements Tool {

    public static class myMapper extends Mapper<LongWritable, Text, CustomizeBean, IntWritable> {
        CustomizeBean outputKey = new CustomizeBean();
        IntWritable outputValue = new IntWritable();

        /**
         * @param key     行的偏移量
         * @param value   行的内容
         * @param context 上下文对象
         * @throws IOException IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split(" ");

            //关键就在于此
            //设置组合键<(key,value),value>
            outputKey.setAll(items[0], Integer.parseInt(items[1]));
            outputValue.set(Integer.parseInt(items[1]));
            context.write(outputKey, outputValue);
        }
    }

    /**
     * Reducer类 输入自定义的java bean 用来封装键值对，做新的键，值做新的值
     */
    public static class myReducer extends Reducer<CustomizeBean, IntWritable, Text, IntWritable> {
        Text outputKey = new Text();

        /**
         * @param key     自定义java bean
         * @param values  同样的键对应的值的迭代器
         * @param context 上下文对象
         * @throws IOException IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void reduce(CustomizeBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                outputKey.set(key.getKeyWord());
                context.write(outputKey, value);
            }
        }
    }

    /**
     * @param args 运行时参数列表
     * @return 返回结束成功与否标志
     * @throws Exception 抛出异常
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(SecondOrderAnother.class);

        //input
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("datas\\Order\\text.txt"));

        //map
        job.setMapperClass(myMapper.class);
        job.setMapOutputKeyClass(CustomizeBean.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle
//        //设置自定义排序使用是在Shuffle阶段
//        job.setSortComparatorClass(UserSort.class);

        //reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(myReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path("datas\\Order\\Ano");
        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : -1;

    }

    /**
     * 程序入口
     *
     * @param args 运行时参数列表
     * @throws Exception 抛出异常JVM处理
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new SecondOrderAnother(), args);
        System.exit(status);
    }
}
