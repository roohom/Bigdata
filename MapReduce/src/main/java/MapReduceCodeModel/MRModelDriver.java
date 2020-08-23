package MapReduceCodeModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName: Model
 * @Author: Roohom
 * @Function: MapReduce变成模板的Driver类
 * @Date: 2020/8/22 12:09
 * @Software: IntelliJ IDEA
 */
public class MRModelDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        //构建一个MapReduce程序
        //实例化MapReduce的Job对象
        Job job = Job.getInstance(this.getConf(), "MRmode");
        job.setJarByClass(MRModelDriver.class);

        //配置MapReduce的五大阶段

        //input
        //设置输入的类，默认是此类，一般用于更改输入类
        job.setInputFormatClass(TextInputFormat.class);
        //配置输入路径
        Path inputPath = new Path(args[0]);
        TextInputFormat.setInputPaths(job, inputPath);

        //map
        //设置mapper类是谁
        job.setMapperClass(null);
        //指定map输出的key类型，也就是K2的类型
        job.setMapOutputKeyClass(null);
        //指定map输出的value类型，也就是V2的类型
        job.setMapOutputValueClass(null);

        //shuffle：分组、排序、分区、Map聚合
//        job.setGroupingComparatorClass(null);
//        job.setSortComparatorClass(null);
//        job.setPartitionerClass(null);
//        job.setCombinerClass(null);

        //reduce
        job.setReducerClass(null);
        //设置reduce的输出key的类型，也就是K3的类型
        job.setOutputKeyClass(null);
        //设置reduce的输出value的类型，也就是V3的类型
        job.setOutputValueClass(null);
        //调节reduce的个数，默认为1
        job.setNumReduceTasks(1);

        //output
        //设置输出类，用于管理输出，默认输出是TextOutputFormat
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(args[1]);
        //设置输出类的保存结果的路径
        TextOutputFormat.setOutputPath(job, outputPath);

        //提交运行
        //三目运算，运行成功状态返回0，否则返回-1
        return job.waitForCompletion(true) ? 0 : -1;
    }


    /**
     * 作为程序的入口
     * 负责调用run
     *
     * @param args 参数
     * @throws Exception 抛出异常
     */

    public static void main(String[] args) throws Exception {
        //构建一个Hadoop配置管理对象
        Configuration conf = new Configuration();
        //使用Hadoop的工具类来调用当前类的run方法
        int status = ToolRunner.run(conf, new MRModelDriver(), args);
        //根据结果进行退出
        System.exit(status);
    }
}
