package SougouWordCount.EveHourTop3;

import com.google.inject.internal.asm.$ClassAdapter;
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
 * @ClassName: SougouSerchHourTop3
 * @Author: Roohom
 * @Function: 统计每小时搜狗搜索数据中的Top3
 * @Date: 2020/8/25 20:41
 * @Software: IntelliJ IDEA
 */

/**
 * 根据搜索内容进行分组聚合 键设置为搜索内容 值设置为1
 * 对值累加 排序取前三
 */
public class SougouSerchHourTop3 extends Configured implements Tool {
    public static class SogouMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //切割出搜索词
            String searchItem = value.toString().split("\t")[2];
            this.outputKey.set(searchItem);
            context.write(this.outputKey, this.outputValue);
        }
    }


    public static class SougouReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable outputValue = new IntWritable();
        Text outputKey = new Text();
        TreeMap<Integer, String> searchMap = new TreeMap<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {

                return o2.compareTo(o1);
            }
        });

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            searchMap.put(sum, key.toString());
            //遍历TreeMap拿出第一个
            for (Integer integer : searchMap.keySet()) {
                outputValue.set(integer);
                break;
            }
            outputKey.set(key);
            context.write(outputKey, outputValue);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(SougouSerchHourTop3.class);

        //input
        job.setInputFormatClass(TextInputFormat.class);
        Path inputpPath = new Path("D:\\itcast\\LinuxAdvanced&Bigdata\\day08\\05_资料书籍\\sougou_data\\SogouQ.reduced");
        TextInputFormat.setInputPaths(job, inputpPath);

        //map
        job.setMapperClass(SogouMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reduce
        job.setReducerClass(SougouReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileSystem hdfs = FileSystem.get(this.getConf());
        Path outputPath = new Path("datas\\sougouOutput\\HourTop3");
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }
        TextOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new SougouSerchHourTop3(), args);
        System.exit(status);
    }
}
