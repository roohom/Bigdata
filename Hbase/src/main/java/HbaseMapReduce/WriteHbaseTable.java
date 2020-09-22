package HbaseMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName: hbaseRead
 * @Author: Roohom
 * @Function: 从本地读取数据写入到hbase的表中
 * @Date: 2020/9/22 19:29
 * @Software: IntelliJ IDEA
 */
public class WriteHbaseTable extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        int status = ToolRunner.run(conf, new WriteHbaseTable(), args);
        System.exit(status);
    }

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf(), "read");
        job.setJarByClass(WriteHbaseTable.class);
        Path inputPath = new Path("datas\\hbase\\input\\upload.txt");
        TextInputFormat.setInputPaths(job, inputPath);
        job.setMapperClass(WriteToHbaseMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Put.class);

        TableMapReduceUtil.initTableReducerJob(
                "student:mrwrite",
                WriteToHbaseReduce.class,
                job
        );
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static class WriteToHbaseMap extends Mapper<LongWritable, Text, Text, Put> {
        Text rowKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String row = split[0];
            String name = split[1];
            String age = split[2];
            String sex = split[3];
            //将id作为rowkey放在key中输出
            this.rowKey.set(row);

            Put putAge = new Put(Bytes.toBytes(row));
            putAge.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age));
            context.write(rowKey, putAge);

            Put putName = new Put(Bytes.toBytes(row));
            putName.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
            context.write(rowKey, putName);


            Put putSex = new Put(Bytes.toBytes(row));
            putSex.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(sex));
            context.write(rowKey, putSex);
        }
    }


    public static class WriteToHbaseReduce extends TableReducer<Text, Put, Text> {
        /**
         * 写入到hbase，相同rowkey的所有put都在一个迭代器中
         *
         * @param key     rowkey
         * @param values  相同rowkey的put
         * @param context 上下文对象
         * @throws IOException          IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            //遍历每个put对象，输出
            for (Put value : values) {
                context.write(key, value);
            }
        }
    }

}
