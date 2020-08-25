package PhoneFlow;

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
 * @ClassName: MRModelTotal
 * @Author: Roohom
 * @Function: MapReduce编程模板
 * @Date: 2020/8/22 18:14
 * @Software: IntelliJ IDEA
 */
public class PhoneFlow extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "MRModel");
        job.setJarByClass(PhoneFlow.class);
        //input
        job.setInputFormatClass(TextInputFormat.class);
        Path inputPath = new Path("datas\\flowCase\\data_flow.dat");
        TextInputFormat.setInputPaths(job, inputPath);

        //map
        job.setMapperClass(MRModelMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //shuffle

        //reduce
        job.setReducerClass(MRModelReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        job.setNumReduceTasks(1);


        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path("datas\\flowCase\\output");
        //判断输出路径是否已存在(使用hdfs的JAVA API)
        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        TextOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new PhoneFlow(), args);
        System.exit(status);
    }

    public static class MRModelMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        Text outputKey = new Text();
        FlowBean outputValue = new FlowBean();

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
            String[] items = value.toString().split("\t");
            if (items.length == 11) {
                this.outputKey.set(items[1]);
                this.outputValue.setAll(Integer.parseInt(items[6]), Integer.parseInt(items[7]), Integer.parseInt(items[8]), Integer.parseInt(items[9]));
                context.write(this.outputKey, this.outputValue);
            } else {
                return;
            }
        }
    }

    public static class MRModelReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
        Text outputKey = new Text();
        FlowBean outputValue = new FlowBean();

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
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            //处理逻辑由需求来定义
            int sumUpPack = 0;
            int sumDownPack = 0;
            int sumUpFlow = 0;
            int sumDownFlow = 0;

            for (FlowBean value : values) {
                sumUpPack += value.getUpPack();
                sumDownPack += value.getDownPack();
                sumUpFlow += value.getUpFlow();
                sumDownFlow += value.getDownFlow();
            }

            this.outputValue.setAll(sumUpPack, sumDownPack, sumUpFlow, sumDownFlow);
            context.write(key, this.outputValue);
        }
    }
}
