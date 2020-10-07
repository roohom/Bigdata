package ESHbaseIntergration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ClassName: HbaseUtil
 * @Author: Roohom
 * @Function: Hbase读写工具类
 * @Date: 2020/9/29 18:10
 * @Software: IntelliJ IDEA
 */
public class HbaseUtil {

    /**
     * 构建连接，获取表对象
     *
     * @param tbName 表名
     * @return 表对象
     * @throws IOException 读取时发生的IO异常
     */
    private static Table getTable(String tbName) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        return conn.getTable(TableName.valueOf(tbName));
    }


    /**
     * 写入数据到HBase
     *
     * @param tbName 表名
     * @param rowkey rowkey
     * @param family 列族
     * @param column 列名
     * @param value  值
     * @throws IOException 可能出现的IO异常
     */
    public static void write(String tbName, String rowkey, String family, String column, String value) throws IOException {
        Table table = getTable(tbName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    /**
     * 从Hbase读取数据
     *
     * @param tbName 表名
     * @param rowkey rowkey
     * @param famliy 列族
     * @param column 列名
     * @return 列值
     * @throws IOException 可能出现的IO异常
     */
    public static String read(String tbName, String rowkey, String famliy, String column) throws IOException {
        Table table = getTable(tbName);
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        byte[] value = result.getValue(Bytes.toBytes(famliy), Bytes.toBytes(column));
        return new String(value);
    }
}
