package SougouWordCount.EveDayTop3;

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
import java.util.TreeMap;


/**
 * @ClassName: SougouSerchHourTop3
 * @Author: Roohom
 * @Function: 统计一天搜狗搜索数据中的Top3
 * @Date: 2020/8/25 20:41
 * @Software: IntelliJ IDEA
 */

/**
 * 根据搜索内容进行分组聚合 键设置为搜索内容 值设置为1
 * 对值累加 排序取前三
 * <p>
 * 每个小时
 * 要根据小时分组 排序
 * 再根据搜索词的数量进行降序排序
 */
public class SougouSerchDayTop3 extends Configured implements Tool {
    public static class SogouMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //切割出搜索词
            String searchItem = value.toString().split("\t")[2];
            outputKey.set(searchItem);
            context.write(outputKey, outputValue);

        }
    }


    public static class SougouReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        TreeMap<UserBean, Integer> searchMap = new TreeMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            UserBean ub = new UserBean();
            ub.setSearchItem(key.toString());
            ub.setCount(sum);
            searchMap.put(ub, sum);
            //如果map的大小超过3，就把第一个删除，始终留下最大的前两个
            if (searchMap.size() > 3) {
                searchMap.remove(searchMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (UserBean userBean : searchMap.keySet()) {
                context.write(new Text(userBean.toString()),new IntWritable(userBean.getCount()));
            }
        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(SougouSerchDayTop3.class);

        //input
        job.setInputFormatClass(TextInputFormat.class);
        Path inputpPath = new Path("D:\\itcast\\LinuxAdvanced&Bigdata\\day08\\05_资料书籍\\sougou_data\\SogouQ.reduced");
//        Path inputpPath = new Path("datas\\sougou_data\\text.txt");
        TextInputFormat.setInputPaths(job, inputpPath);

        //map
        job.setMapperClass(SogouMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle
//        job.setGroupingComparatorClass(UserSort.class);

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
        int status = ToolRunner.run(conf, new SougouSerchDayTop3(), args);
        System.exit(status);
    }
}
