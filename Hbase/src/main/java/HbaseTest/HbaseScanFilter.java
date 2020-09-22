package HbaseTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ClassName: HbaseTest.HbaseScanFilter
 * @Author: Roohom
 * @Function: 使用HBASE API测试扫描数据 为scan添加过滤器
 * @Date: 2020/9/22 15:01
 * @Software: IntelliJ IDEA
 */
public class HbaseScanFilter {
    public static void main(String[] args) throws IOException {
        //实例化一个对象
        HbaseScanFilter scanFilter = new HbaseScanFilter();
        //构建连接
        Connection conn = scanFilter.getConnect();
        //对表的操作，构建一个表的对象
        Table table = scanFilter.getTable(conn);
        //起始范围过滤扫描数据
//        scanFilter.scanData(table);
        //根据行键扫描数据
//        scanFilter.scanRowkey(table);
        //根据列族来查询数据
//        scanFilter.scanFamily(table);
        //根据列标签来查询数据
//        scanFilter.scanQualifier(table);
        //根据列值来查询数据
//        scanFilter.scanValue(table);
        //单列值过滤器，查询名叫xxx的数据的信息
        scanFilter.scanSingleColVal(table);
        //使用多列前缀比较器来过滤查询
//        scanFilter.scanMultiColPre(table);
        //使用过滤器列表来组合查询
//        scanFilter.scanFilterList(table);

    }


    /**
     * 配置起始范围过滤器的scan
     *
     * @param table 表对象
     * @throws IOException IO异常
     */
    public void scanData(Table table) throws IOException {
        //实例化一个scan对象
        Scan scan = new Scan();
        /*
         *配置Scan过滤器*
         */
        //起始范围过滤
        scan.withStartRow(Bytes.toBytes("20200920_001"));
        scan.withStopRow(Bytes.toBytes("20200920_003"));
        //执行Scan
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        Bytes.toString(result.getRow()) + "\t" +
                                Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
                                cell.getTimestamp()
                );
            }
        }
    }

    /**
     * 根据给定的rowkey扫描数据
     *
     * @param table 表对象
     * @throws IOException IO异常
     */
    public void scanRowkey(Table table) throws IOException {
        Scan scan = new Scan();
        RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator("20200920"));
        scan.setFilter(rowFilter);
        //执行Scan
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        Bytes.toString(result.getRow()) + "\t" +
                                Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
                                cell.getTimestamp()
                );
            }
        }
    }

    /**
     * 根据给定的列族查询数据
     *
     * @param table 表兑现
     * @throws IOException IO异常
     */
    public void scanFamily(Table table) throws IOException {
        Scan scan = new Scan();
        FamilyFilter familyFilter = new FamilyFilter(CompareOperator.EQUAL, new SubstringComparator("basic"));
        scan.setFilter(familyFilter);
        //执行Scan
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        Bytes.toString(result.getRow()) + "\t" +
                                Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
                                cell.getTimestamp()
                );
            }
        }

    }

    /**
     * 根据列标签查询数据
     *
     * @param table 表对象
     * @throws IOException IO异常
     */
    public void scanQualifier(Table table) throws IOException {
        Scan scan = new Scan();
        QualifierFilter qualifierFilter = new QualifierFilter(CompareOperator.EQUAL, new SubstringComparator("name"));
        scan.setFilter(qualifierFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        Bytes.toString(result.getRow()) + "\t" +
                                Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
                                cell.getTimestamp()
                );
            }
        }
    }

    /**
     * 根据列值来查询数据
     *
     * @param table 表对象
     * @throws IOException IO异常
     */
    public void scanValue(Table table) throws IOException {
        Scan scan = new Scan();
        ValueFilter valueFilter = new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("e"));
        scan.setFilter(valueFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        Bytes.toString(result.getRow()) + "\t" +
                                Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
                                cell.getTimestamp()
                );
            }
        }
    }

    public void scanSingleColVal(Table table) throws IOException {
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes("basic"),
                Bytes.toBytes("name"),
                CompareOperator.EQUAL,
                Bytes.toBytes("laoda")
        );
//        singleColumnValueFilter.setFilterIfMissing(true);
        scan.setFilter(singleColumnValueFilter);
//        scan.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("name"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        Bytes.toString(result.getRow()) + "\t" +
                                Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
                                cell.getTimestamp()
                );
            }
        }
    }

    /**
     * 多列前缀比较器
     *
     * @param table 表对象
     * @throws IOException IO异常
     */
    public void scanMultiColPre(Table table) throws IOException {
        Scan scan = new Scan();
        byte[][] prefix = {
                Bytes.toBytes("age"),
                Bytes.toBytes("sex")
        };
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(prefix);
        scan.setFilter(multipleColumnPrefixFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        Bytes.toString(result.getRow()) + "\t" +
                                Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
                                cell.getTimestamp()
                );
            }
        }
    }

    public void scanFilterList(Table table) throws IOException {
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes("basic"),
                Bytes.toBytes("name"),
                CompareOperator.EQUAL,
                Bytes.toBytes("laoda")
        );
        byte[][] prefix = {
                Bytes.toBytes("age"),
                Bytes.toBytes("sex")
        };

        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(prefix);
        FilterList filterList = new FilterList();
        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(multipleColumnPrefixFilter);
        scan.setFilter(filterList);
//        scan.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("name"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        Bytes.toString(result.getRow()) + "\t" +
                                Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
                                Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
                                cell.getTimestamp()
                );
            }
        }
    }


    /**
     * 获取表对象
     *
     * @param connection HBASE连接对象
     * @return 返回表
     * @throws IOException IO异常
     */
    public Table getTable(Connection connection) throws IOException {
        return connection.getTable(TableName.valueOf("student:stu_info"));
    }


    public Connection getConnect() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        return ConnectionFactory.createConnection(conf);
    }
}
