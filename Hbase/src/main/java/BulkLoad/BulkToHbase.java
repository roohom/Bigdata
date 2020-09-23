package BulkLoad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;

import java.io.IOException;

/**
 * @ClassName: BulkToHbase
 * @Author: Roohom
 * @Function: 将已转换好的 HFILE文件导入Hbase的表中
 * @Date: 2020/9/23 20:49
 * @Software: IntelliJ IDEA
 */
public class BulkToHbase {
    public static void main(String[] args) throws IOException {
        //构件连接
        Configuration conf = HBaseConfiguration.create();
        //设置存储路径为HDFS本地
        conf.set("fs.defaultFS","hdfs://node1:8020");
        //向zookeeper请求连接属性配置
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        //指定HFILE文件的地址
        //Windows本地运行
        Path readPath = new Path("/hbase/output");
//        配置在Linux运行开启
//        Path hfile = new Path(args[0]);
        //获取管理员对象
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        //获取表的对象
        Table table = conn.getTable(TableName.valueOf("mrhbase2"));
        //获取该表的region
        RegionLocator locator = conn.getRegionLocator(TableName.valueOf("mrhbase2"));
        //加载实现的对象
        LoadIncrementalHFiles hFiles = new LoadIncrementalHFiles(conf);
        //导入到HBASE表的方法
        hFiles.doBulkLoad(
                readPath,
                admin,
                table,
                locator
        );
    }
}
