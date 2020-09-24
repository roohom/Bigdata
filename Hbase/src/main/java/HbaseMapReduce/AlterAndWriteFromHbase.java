package HbaseMapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ClassName: AlterAndWriteFromHbase
 * @Author: Roohom
 * @Function: 为一张表添加列族 TODO:待完成
 * @Date: 2020/9/22 21:55
 * @Software: IntelliJ IDEA
 */
public class AlterAndWriteFromHbase {
    public static void main(String[] args) throws IOException {
        AlterAndWriteFromHbase writeFromHbase = new AlterAndWriteFromHbase();
        Connection conn = writeFromHbase.getConnect();
        HBaseAdmin admin = writeFromHbase.getAdmin(conn);
        writeFromHbase.alterTable(admin);
    }


    public void alterTable(HBaseAdmin admin) throws IOException {
        TableName tableName = TableName.valueOf("student:mrwrite");
        admin.disableTable(tableName);
        HTableDescriptor descriptor = admin.getTableDescriptor(tableName);
        HColumnDescriptor basic = new HColumnDescriptor("basic");
        descriptor.addFamily(basic);
    }
    public HBaseAdmin getAdmin(Connection conn) throws IOException {
        return (HBaseAdmin) conn.getAdmin();
    }

    public Connection getConnect() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181,");
        return ConnectionFactory.createConnection(conf);
    }
}
