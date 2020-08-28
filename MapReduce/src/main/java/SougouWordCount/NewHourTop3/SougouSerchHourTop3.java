package SougouWordCount.NewHourTop3;

import SougouWordCount.EveHourTop3.SortBean;
import SougouWordCount.EveHourTop3.UserBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import java.util.TreeSet;


/**
 * @ClassName: SougouSerchHourTop3
 * @Author: Roohom
 * @Function: 统计每个小时搜狗搜索数据中的Top3
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
public class SougouSerchHourTop3 extends Configured implements Tool {
    public static class SogouMapper extends Mapper<LongWritable, Text, UserBean, IntWritable> {
        UserBean outputKey = new UserBean();
        IntWritable outputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //切割出搜索词
            String searchItem = value.toString().split("\t")[2];
            String hour = value.toString().substring(0, 2);
            outputKey.setHour(Integer.parseInt(hour));
            outputKey.setSearchItem(searchItem);
            //userbean中没有插入搜索量，因为目前没有搜索量产生
            context.write(this.outputKey, this.outputValue);
        }
    }

    //map端输出 userbean 按照 时间 搜索量排序
    public static class SougouReducer extends Reducer<UserBean, IntWritable, Text, NullWritable> {
        TreeSet<String> searchSet = new TreeSet<>(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });

//        TreeMap<Integer,String> searchMap = new TreeMap<>();

        Text outputKey = new Text();
        NullWritable outputValue = NullWritable.get();

        @Override
        protected void reduce(UserBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            //设置搜索词的搜索量
            key.setCount(sum);
            searchSet.add(key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (String searchItem : searchSet) {
                //跳坑警告！！！！！！！
                this.outputKey.set(searchItem);
                context.write(this.outputKey, this.outputValue);
            }
        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(SougouSerchHourTop3.class);

        //input
        job.setInputFormatClass(TextInputFormat.class);
//        Path inputpPath = new Path("D:\\itcast\\LinuxAdvanced&Bigdata\\day08\\05_资料书籍\\sougou_data\\SogouQ.reduced");
        Path inputpPath = new Path("datas\\sougou_data\\text.txt");
        TextInputFormat.setInputPaths(job, inputpPath);

        //map
        job.setMapperClass(SogouMapper.class);
        job.setMapOutputKeyClass(UserBean.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle

        //reduce
        job.setReducerClass(SougouReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileSystem hdfs = FileSystem.get(this.getConf());
        Path outputPath = new Path("datas\\sougouOutput\\HourTop3\\newOut");
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
