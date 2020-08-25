package SecondHouseCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

/**
 * @ClassName: SecHouseAvgHighLow
 * @Author: Roohom
 * @Function: 分析出每个地区的最高房价 最低房价 平均房价
 * @Date: 2020/8/25 10:43
 * @Software: IntelliJ IDEA
 */

/**
 * 分析
 * 显示地区和该地区房价的最高价
 */
public class SecHouseAvgHighLow extends Configured implements Tool {

    public static class SecMapper extends Mapper<LongWritable, Text, CountBean, NullWritable> {
        CountBean outputKey = new CountBean();
        NullWritable outputValue = NullWritable.get();

        /**
         * @param key     行的偏移量
         * @param value   行的内容
         * @param context 上下文
         * @throws IOException IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String price = value.toString().split(",")[6];
            String region = value.toString().split(",")[3];
            this.outputKey.setPrice(Integer.parseInt(price));
            this.outputKey.setRegion(region);
            context.write(this.outputKey, this.outputValue);
        }
    }

    public static class SecReducer extends Reducer<CountBean, NullWritable, Text, Text> {
        Text outputValue = new Text();
        Text outputKey = new Text();

        /**
         * 按照价格排序之后按照地区分组，取出第一条即是房价最高的一条
         *
         * @param key     自定义java bean封装了地区和房价
         * @param values  空值
         * @param context 上下文
         * @throws IOException          IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void reduce(CountBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            //房的总个数
            int cnt = 0;
            int max = 0;
            int min = 99999999;
            //总房价
            int sum = 0;
            for (NullWritable value : values) {
                max = Math.max(max, key.getPrice());
                min = Math.min(min, key.getPrice());
                cnt++;
                sum+= key.getPrice();
            }
            outputKey.set(key.getRegion());
            outputValue.set(max + "万\t" + min + "万\t"+(sum/cnt)+"万\t");
            context.write(outputKey, outputValue);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf(), "SecHouse");
        job.setJarByClass(SecHouseAvgHighLow.class);

        //input
        TextInputFormat.setInputPaths(job, new Path("datas\\lianjia\\secondhouse.csv"));

        //map
        job.setMapperClass(SecMapper.class);
        job.setMapOutputKeyClass(CountBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //shffle
        //自定义分组比较器进行分组
        job.setGroupingComparatorClass(OrderGroup.class);

        //reduce
        job.setReducerClass(SecReducer.class);
        job.setOutputKeyClass(CountBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path("datas\\HouseOutput\\Bean");
        TextOutputFormat.setOutputPath(job, outputPath);

        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        //output
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new SecHouseAvgHighLow(), args);
        System.exit(status);
    }
}
