package SecondHouseCount;

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
 * @ClassName: HouseSortByAvg
 * @Author: Roohom
 * @Function: 根据平均房价来对各个地区进行排序
 * @Date: 2020/8/27 19:52
 * @Software: IntelliJ IDEA
 */
public class HouseSortByAvg extends Configured implements Tool {

    public static class avgMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable();


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String price = value.toString().split(",")[6];
            String region = value.toString().split(",")[3];
            outputKey.set(region);
            outputValue.set(Integer.parseInt(price));
            context.write(outputKey, outputValue);
        }
    }


    public static class avgReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable();
        TreeMap<Integer,String> avgMap = new TreeMap<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o2-o1;
            }
        });

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            int sum = 0;
            for (IntWritable value : values) {
                cnt++;
                sum+=value.get();
            }
            avgMap.put(sum/cnt,key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer integer : avgMap.keySet()) {
                outputKey.set(avgMap.get(integer));
                outputValue.set(integer);
                context.write(outputKey,outputValue);
            }
        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(HouseSortByAvg.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job,new Path("datas\\lianjia\\secondhouse.csv"));

        job.setMapperClass(avgMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(avgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path("datas\\HouseOutput\\SortByAvg");
        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath))
        {
            hdfs.delete(outputPath,true);
        }
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,outputPath);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new HouseSortByAvg(), args);
        System.exit(status);
    }
}
