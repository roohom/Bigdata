package HbaseTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ClassName: HbaseTest.HbaseDMLTest
 * @Author: Roohom
 * @Function: 测试HBASE DML语句
 * @Date: 2020/9/21 21:19
 * @Software: IntelliJ IDEA
 */
public class HbaseDMLTest {
    public static void main(String[] args) throws IOException {
        //构建当前类的对象
        HbaseDMLTest dmlTest = new HbaseDMLTest();
        Connection conn = dmlTest.getConnect();
        //对表的数据操作，需要构建一个表的对象
        Table table = dmlTest.getTable(conn);
        //扫描数据
        dmlTest.scanData(table);
        //插入数据
//        dmlTest.putData(table);
        //获取数据
//        dmlTest.getData(table);
        //删除数据
//        dmlTest.deleteData(table,"20200921_001");
    }

    /**
     * 扫描表
     *
     * @param table 表对象
     * @throws IOException IO异常
     */
    public void scanData(Table table) throws IOException {
        //构建Scan对象的实例
        Scan scan = new Scan();
        //执行scan
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
                                cell.getTimestamp()
                );
            }
            System.out.println("================================================");
        }
    }

    /**
     * 删除表
     * @param table 表对象
     * @param rowKey 行键，在调用时指定
     * @throws IOException IO异常
     */
    public void deleteData(Table table,String rowKey) throws IOException {
        //构建delete实例
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //只删除最新版本
        delete.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("name"));
        //删除所有版本
//        delete.addColumns(Bytes.toBytes("basic"),Bytes.toBytes("name"));
        table.delete(delete);
    }

    /**
     * 获取数据
     *
     * @param table 表对象
     * @throws IOException IO异常
     */
    public void getData(Table table) throws IOException {
        Get get = new Get(Bytes.toBytes("20200920_001"));
//        get.addFamily(Bytes.toBytes("basic"));
        get.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("name"));
        //执行get
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(
                    Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +
                            Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                            Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                            Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
                            cell.getTimestamp()
            );
        }
    }


    /**
     * 添加数据
     *
     * @param table 表对象
     * @throws IOException IO异常
     */
    public void putData(Table table) throws IOException {
        Put put = new Put(Bytes.toBytes("20200921_004"));
        put.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("name"), Bytes.toBytes("laowu"));
        table.put(put);
    }


    /**
     * 获取连接
     *
     * @return 连接对象
     * @throws IOException IO异常
     */
    public Connection getConnect() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        return ConnectionFactory.createConnection(conf);
    }

    public Table getTable(Connection conn) throws IOException {
        return conn.getTable(TableName.valueOf("student:stu_info"));
    }

}
