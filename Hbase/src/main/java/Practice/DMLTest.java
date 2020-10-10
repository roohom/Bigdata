package Practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

/**
 * @ClassName: DMLTest
 * @Author: Roohom
 * @Function: DML
 * @Date: 2020/10/9 21:20
 * @Software: IntelliJ IDEA
 */
public class DMLTest {
    //获取连接
    public Connection getConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        return ConnectionFactory.createConnection(conf);
    }

    //获取管理员对象
    public HBaseAdmin getAdmin(Connection connection) throws IOException {
        return (HBaseAdmin) connection.getAdmin();
    }

    //获取表连接
    public Table getTable(Connection connection) throws IOException {
        return connection.getTable(TableName.valueOf("hbase_score"));
    }

    //查询数据Scan
    @Test
    public void scanData() throws IOException {
        Table table = getTable(getConnection());
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))+"\t"+
                        Bytes.toString(CellUtil.cloneFamily(cell))+"\t"+
                        Bytes.toString(CellUtil.cloneQualifier(cell))+"\t"+
                        Bytes.toString(CellUtil.cloneValue(cell))+"\t"+
                        cell.getTimestamp());
            }
        }
    }

    //添加数据
    @Test
    public void putData() throws IOException {
        Table table = getTable(getConnection());
        Put put = new Put(Bytes.toBytes("20201009_002"));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("score"),Bytes.toBytes("90"));
        table.put(put);
    }

    //删除数据
    @Test
    public void deleteData() throws IOException {
        Table table = getTable(getConnection());
        Delete delete = new Delete(Bytes.toBytes("20201009_002"));
//        delete.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("name"));
        table.delete(delete);
    }

    //获取数据get方式
    @Test
    public void getData() throws IOException {
        Table table = getTable(getConnection());
        Get get = new Get(Bytes.toBytes("1"));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell))+"\t"+
                    Bytes.toString(CellUtil.cloneFamily(cell))+"\t"+
                    Bytes.toString(CellUtil.cloneQualifier(cell))+"\t"+
                    Bytes.toString(CellUtil.cloneValue(cell))+"\t"+
                    cell.getTimestamp());
        }
    }
}
