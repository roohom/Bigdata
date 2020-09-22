package HbaseMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName: ReadHbaseWriteAno
 * @Author: Roohom
 * @Function: 从hbase的一张表读取数据写入到领一张表
 * @Date: 2020/9/22 20:57
 * @Software: IntelliJ IDEA
 */
public class ReadHbaseWriteAno extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        int status = ToolRunner.run(conf, new ReadHbaseWriteAno(), args);
        System.exit(status);
    }

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf(), "readAndWrite");
        job.setJarByClass(ReadHbaseWriteAno.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(
                "student:stu_info",
                scan,
                ReadHbaseMapAno.class,
                Text.class,
                Put.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                "student:mrwrite2",
                ReadHbaseReduceAno.class,
                job
        );

        return job.waitForCompletion(true) ? 0 : -1;
    }


    public static class ReadHbaseMapAno extends TableMapper<Text, Put> {
        Text outputKey = new Text();

        /**
         * 从hbase读取数据的map
         *
         * @param key rowkey
         * @param value 结果集
         * @param context 上下文
         * @throws IOException IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String rowKey = Bytes.toString(key.get());
            outputKey.set(rowKey);
            this.outputKey.set(rowKey);
            for (Cell cell : value.rawCells()) {
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
                context.write(outputKey, put);
            }
        }
    }

    public static class ReadHbaseReduceAno extends TableReducer<Text, Put, Text> {
        /**
         * 写入到hbase，相同rowkey的put都在迭代器中
         *
         * @param key     rowkey
         * @param values  put迭代器，存储需要提交的每一个put
         * @param context 山下文对象
         * @throws IOException          IO异常
         * @throws InterruptedException 中断异常
         */
        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            for (Put value : values) {
                context.write(key, value);
            }
        }
    }
}
