package TopN;

import com.sun.org.apache.regexp.internal.RE;
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
import java.util.Comparator;
import java.util.TreeMap;

/**
 * @ClassName: ShuffleTopN
 * @Author: Roohom
 * @Function: 分析各地区房价最低价，升级写法，对房价的排序放在Reduce里
 * @Date: 2020/8/25 19:11
 * @Software: IntelliJ IDEA
 */
public class ShuffleTopN extends Configured implements Tool {

    public static class topMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //地区
            String region = value.toString().split(",")[3];
            //房价
            String price = value.toString().split(",")[6];
            this.outputKey.set(region);
            this.outputValue.set(Integer.parseInt(price));
            context.write(outputKey, outputValue);
        }
    }

    public static class topReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable();
        TreeMap<Integer, String> houseMap = new TreeMap<>();

        /**
         * @param key     地区
         * @param values  存储了该地区所有房子房价的迭代器
         * @param context 上下文
         * @throws IOException          IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            //把房价作为键，地区作为值装入TreeMap
            for (IntWritable value : values) {
                houseMap.put(value.get(), key.toString());
            }
            for (Integer mapKey : houseMap.keySet()) {
                this.outputValue.set(mapKey);
                break;
            }
            context.write(key, this.outputValue);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "HousePriceTop");
        job.setJarByClass(ShuffleTopN.class);

        //input
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, new Path("datas\\lianjia\\secondhouse.csv"));

        //map
        job.setMapperClass(topMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle

        //reduce
        job.setReducerClass(topReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path("datas\\HouseOutput\\TopN");
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
        int status = ToolRunner.run(conf, new ShuffleTopN(), args);
        System.exit(status);
    }
}
