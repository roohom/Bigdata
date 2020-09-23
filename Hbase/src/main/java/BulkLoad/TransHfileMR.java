package BulkLoad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName: TransHfileMR
 * @Author: Roohom
 * @Function: 讲一个普通的数据文件转换成为一个HFILE文件
 * @Date: 2020/9/23 19:09
 * @Software: IntelliJ IDEA
 */
public class TransHfileMR extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        //构建job
        Job job = Job.getInstance(this.getConf(), "BulkLoad");
        job.setJarByClass(TransHfileMR.class);

        //设置输入路径
        Path inputPath = new Path("/user/hive/warehouse/input");
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileInputFormat.setInputPaths(job, inputPath);

        //设置map，将普通文件中的数据封装成put对象，用于构建HFILE文件
        job.setMapperClass(ToHfileMap.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //reducer个数
        job.setNumReduceTasks(0);

        Path outputPath = new Path("/hbase/output");
        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        //output将输出的数据变成HFILE文件
        job.setOutputFormatClass(HFileOutputFormat2.class);
        //设置输出的HFILE文件的存放位置
//        HFileOutputFormat2.setOutputPath(job, new Path(args[1]));
        HFileOutputFormat2.setOutputPath(job, outputPath);

        //配置获取连接
        Connection conn = ConnectionFactory.createConnection(this.getConf());
        //定义最终该Hfile文件要导入哪张表
        Table table = conn.getTable(TableName.valueOf("student:mrwrite2"));
        //获取该表对应的region的位置，用于将文件放入对应的region中
        RegionLocator locator = conn.getRegionLocator(TableName.valueOf("student:mrwrite2"));
        //指定将该HFILE文件最终导入到这张表的这个region中
        HFileOutputFormat2.configureIncrementalLoad(job, table, locator);

        //submit
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://node1:8020");
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        int status = ToolRunner.run(conf, new TransHfileMR(), args);
        System.exit(status);
    }

    public static class ToHfileMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
        byte[] family = Bytes.toBytes("info");

        /**
         * Map
         *
         * @param key     行偏移量
         * @param value   该行数据
         * @param context 上下文
         * @throws IOException          IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将一行数据转换为字符串
            String line = value.toString();
            //对这一行数据以\t分割
            String[] items = line.split("\t");
            //将第一列作为rowkey
            String rowKey = items[0];
            this.outputKey.set(rowKey.getBytes());
            Put outputValue = new Put(this.outputKey.get());

            //将name这一列放入put
            String name = items[1];
            outputValue.addColumn(family, "name".getBytes(), name.getBytes());
            context.write(this.outputKey, outputValue);

            //将age这一列放入put
            String age = items[2];
            outputValue.addColumn(family, "age".getBytes(), age.getBytes());
            context.write(this.outputKey, outputValue);

            //将sex这一列放入put
            String sex = items[3];
            outputValue.addColumn(family, "sex".getBytes(), sex.getBytes());
            context.write(this.outputKey, outputValue);
        }
    }

}
