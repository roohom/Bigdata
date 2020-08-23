package TLCDataAnalize;

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
 * @ClassName: TLCTaxiPayment
 * @Author: Roohom
 * @Function: 统计TLC公开的数据中出租车使用各种支付方式的用户数
 * @Date: 2020/8/23 15:43
 * @Software: IntelliJ IDEA
 */
public class TLCTaxiPayment extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "TLCPayment");
        job.setJarByClass(TLCTaxiPayment.class);

        Path inputPath = new Path(args[0]);
        TextInputFormat.setInputPaths(job, inputPath);

        job.setMapperClass(TlcMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TlcReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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
        int status = ToolRunner.run(conf, new TLCTaxiPayment(), args);
        System.exit(status);
    }

    public static class TlcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String payment = value.toString().split(",")[9];
            this.outputKey.set(payment);
            context.write(this.outputKey, this.outputValue);
        }
    }

    public static class TlcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable();
        Text key1 = new Text("1");
        Text key2 = new Text("2");
        Text key3 = new Text("3");
        Text key4 = new Text("4");
        Text key5 = new Text("5");
        Text key6 = new Text("6");


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            this.outputValue.set(sum);
//            context.write(key,this.outputValue);
            if (key.equals(key1)) {
                outputKey.set("Credit Card");
                context.write(outputKey, this.outputValue);
            }
            if (key.equals(key2)) {
                outputKey.set("Cash");
                context.write(outputKey, this.outputValue);
            }
            if (key.equals(key3)) {
                outputKey.set("No Charge");
                context.write(outputKey, this.outputValue);
            }
            if (key.equals(key4)) {
                outputKey.set("Dispute");
                context.write(outputKey, this.outputValue);
            }
            if (key.equals(key5)) {
                outputKey.set("Unknown");
                context.write(outputKey, this.outputValue);
            }
            if (key.equals(key6)) {
                outputKey.set("Voided Trip");
                context.write(outputKey, this.outputValue);
            }
        }
    }
}
