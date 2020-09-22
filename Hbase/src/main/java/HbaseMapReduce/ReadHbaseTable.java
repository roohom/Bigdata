package HbaseMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName: hbaseRead
 * @Author: Roohom
 * @Function: 从hbase的表读取数据写入文件
 * @Date: 2020/9/22 19:29
 * @Software: IntelliJ IDEA
 */
public class ReadHbaseTable extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        int status = ToolRunner.run(conf, new ReadHbaseTable(),args);
        System.exit(status);
    }

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf(),"read");
        job.setJarByClass(ReadHbaseTable.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(
                "student:stu_info",
                scan,
                ReadHbaseMap.class,
                Text.class,
                Text.class,
                job
        );
        job.setNumReduceTasks(0);

        Path outputPath = new Path("datas\\hbase");
        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(outputPath))
        {
            hdfs.delete(outputPath,true);
        }
        TextOutputFormat.setOutputPath(job,outputPath);
        return job.waitForCompletion(true)?0:-1;
    }



    public static class ReadHbaseMap extends TableMapper<Text,Text>
    {
        //rowkey
        Text outputKey =new Text();
        //每一列的数据
        Text outputValue = new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String rowKey = Bytes.toString(key.get());
            this.outputKey.set(rowKey);
            for (Cell cell : value.rawCells()) {
                String famliy = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String val = Bytes.toString(CellUtil.cloneValue(cell));
                long timestamp = cell.getTimestamp();
                this.outputValue.set(famliy+"\t"+column+"\t"+val+"\t"+timestamp);
                context.write(outputKey,outputValue);
            }
        }
    }
}
