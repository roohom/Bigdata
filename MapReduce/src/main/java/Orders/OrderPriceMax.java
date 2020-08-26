package Orders;

import SougouWordCount.EveHourTop3.UserBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.jws.soap.SOAPBinding;
import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * @ClassName: OrderPriceMax
 * @Author: Roohom
 * @Function: 获取每个订单里最大的价格的那条数据
 * @Date: 2020/8/26 17:45
 * @Software: IntelliJ IDEA
 */
public class OrderPriceMax extends Configured implements Tool {
    public static class OrderMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        Text ouputKey = new Text();
        DoubleWritable outputValue = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("\t");
            ouputKey.set(items[0]);
            outputValue.set(Double.parseDouble(items[2]));
            context.write(ouputKey, outputValue);
        }
    }

    public static class OrderReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        TreeSet<Double> priceSet = new TreeSet<>(new Comparator<Double>() {
            @Override
            public int compare(Double o1, Double o2) {
                return o1.compareTo(o2);
            }
        });
        Text outputKey = new Text();
        DoubleWritable outputValue = new DoubleWritable();


        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                priceSet.add(value.get());
            }
            outputKey.set(key);
            for (Double aDouble : priceSet) {
                outputValue.set(aDouble);
            }
            context.write(outputKey, outputValue);
            priceSet.clear();
        }
    }




    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(OrderPriceMax.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("datas\\orders\\orders.txt"));

        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        Path outputPath = new Path("datas\\orders\\output");
        job.setOutputFormatClass(TextOutputFormat.class);
        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }
        TextOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : -1;


    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new OrderPriceMax(), args);
        System.exit(status);

    }
}
