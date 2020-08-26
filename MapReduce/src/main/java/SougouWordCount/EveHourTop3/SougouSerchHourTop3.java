package SougouWordCount.EveHourTop3;

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
        TreeSet<UserBean> searchSet = new TreeSet<>(new Comparator<UserBean>() {
            @Override
            public int compare(UserBean o1, UserBean o2) {
                return o2.getSearchItem().compareTo(o1.getSearchItem());
            }
        });

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
            //将userbean作为键 搜索量作为值 装入searchMap
            searchSet.add(key);
            for (UserBean userBean : searchSet) {
                this.outputKey.set(userBean.toString());
                context.write(this.outputKey, this.outputValue);
            }

        }
    }

    public static class TopMapper extends Mapper<LongWritable, Text, IntWritable, SortBean> {
        IntWritable outputKey = new IntWritable();
        SortBean outputValue = new SortBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            this.outputKey.set(Integer.parseInt(splits[0]));
            this.outputValue.setSearchCount(Integer.parseInt(splits[1]));
            this.outputValue.setSearchItem(splits[2]);
            context.write(this.outputKey, this.outputValue);
        }
    }


    public static class TopReducer extends Reducer<IntWritable, SortBean, IntWritable, Text> {
        Text outputValue = new Text();
//        IntWritable outputKey = new IntWritable();
        TreeMap<Integer,String> treeMap = new TreeMap<>();
        IntWritable outputKey = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<SortBean> values, Context context) throws IOException, InterruptedException {
            for (SortBean value : values) {
                treeMap.put(value.getSearchCount(),value.getSearchItem());
                if (treeMap.size()>3)
                {
                    treeMap.remove(treeMap.firstKey());
                }
            }
            for (Integer integer : treeMap.keySet()) {
                outputKey.set(key.get());
                outputValue.set(integer+"\t"+treeMap.get(integer));
                context.write(outputKey,outputValue);
            }
            treeMap.clear();
        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(SougouSerchHourTop3.class);

        //input
        job.setInputFormatClass(TextInputFormat.class);
        Path inputpPath = new Path("D:\\itcast\\LinuxAdvanced&Bigdata\\day08\\05_资料书籍\\sougou_data\\SogouQ.reduced");
//        Path inputpPath = new Path("datas\\sougou_data\\text.txt");
        TextInputFormat.setInputPaths(job, inputpPath);

        //map
        job.setMapperClass(SogouMapper.class);
        job.setMapOutputKeyClass(UserBean.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle
//        job.setGroupingComparatorClass(UserGroup.class);

        //reduce
        job.setReducerClass(SougouReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileSystem hdfs = FileSystem.get(this.getConf());
        Path outputPath = new Path("datas\\sougouOutput\\HourTop3");
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }
        TextOutputFormat.setOutputPath(job, outputPath);

        Job jobTwo = Job.getInstance(this.getConf());
        jobTwo.setJarByClass(SougouSerchHourTop3.class);
        jobTwo.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(jobTwo, new Path("datas\\sougouOutput\\HourTop3\\part-r-00000"));

        jobTwo.setMapperClass(TopMapper.class);
        jobTwo.setMapOutputKeyClass(IntWritable.class);
        jobTwo.setMapOutputValueClass(SortBean.class);

        jobTwo.setReducerClass(TopReducer.class);
        jobTwo.setOutputKeyClass(IntWritable.class);
        jobTwo.setOutputValueClass(Text.class);

        jobTwo.setOutputFormatClass(TextOutputFormat.class);
        Path twoOutputPath = new Path("datas\\sougouOutput\\HourTop3\\FinalOut");
        FileSystem hdfsTwo = FileSystem.get(this.getConf());
        if (hdfsTwo.exists(twoOutputPath)) {
            hdfsTwo.delete(twoOutputPath, true);
        }
        TextOutputFormat.setOutputPath(jobTwo, twoOutputPath);

        if (job.waitForCompletion(true)) {
            return jobTwo.waitForCompletion(true) ? 0 : -1;
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new SougouSerchHourTop3(), args);
        System.exit(status);
    }
}
