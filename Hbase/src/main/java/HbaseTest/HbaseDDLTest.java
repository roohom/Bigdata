package HbaseTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName: HbaseDDLCreate
 * @Author: Roohom
 * @Function: 测试HBASE DDL语句
 * @Date: 2020/9/21 17:13
 * @Software: IntelliJ IDEA
 */
public class HbaseDDLTest {

    //创建连接
    public Connection getConnect() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181,");
        return ConnectionFactory.createConnection(conf);
    }


    public static void main(String[] args) throws IOException {
        //实例化当前类的对象
        HbaseDDLTest ddlCreate = new HbaseDDLTest();
        //构建连接
        Connection conn = ddlCreate.getConnect();
        //构建管理员对象
        HBaseAdmin admin = ddlCreate.getAdmin(conn);
        System.out.println("===============创建表===============");
        ddlCreate.createTable(admin);
        System.out.println("==========创建自定义表及列族==========");
        ddlCreate.createTableUserDefine(admin, "ns1:stuinfo", "basic");
        System.out.println("=============列出所有的表============");
        ddlCreate.listTables(admin);
        System.out.println("=============创建NS=============");
        ddlCreate.createNs(admin);
        System.out.println("==========列出所有NameSpace==========");
        ddlCreate.listAllNs(admin);
        System.out.println("============删除NameSpace===========");
        ddlCreate.dropNs(admin);


    }

    /**
     * Func: 创建表
     *
     * @param admin 管理员对象
     * @throws IOException IO异常
     */
    public void createTable(HBaseAdmin admin) throws IOException {
        TableName tableName = TableName.valueOf("ns1:bigdata03");
        if (admin.tableExists(tableName)) {
            //先禁用
            admin.disableTable(tableName);
            //再删除
            admin.deleteTable(tableName);
        }
        //构建表的建造器
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        //构建列族描述器
        ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).setMaxVersions(3).build();
        //添加列族
        builder.setColumnFamily(family);
        TableDescriptor desc = builder.build();
        admin.createTable(desc);
    }

    /**
     * 用户自定义表名、列族 进行创建
     *
     * @param admin      管理员对象
     * @param sTableName 自定义表名
     * @param sFamily    自定义列族
     * @throws IOException IO异常
     */
    public void createTableUserDefine(HBaseAdmin admin, String sTableName, String sFamily) throws IOException {
        TableName tableName = TableName.valueOf(sTableName);
        if (admin.tableExists(tableName)) {
            //先禁用
            admin.disableTable(tableName);
            //再删除
            admin.deleteTable(tableName);
        }
        //构建表的建造器
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        //构建列族描述器
        ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(sFamily)).setMaxVersions(3).build();
        //添加列族
        builder.setColumnFamily(family);

        TableDescriptor desc = builder.build();
        admin.createTable(desc);
    }

    /**
     * 列举表
     *
     * @param admin 管理员对象
     * @throws IOException IO异常
     */
    public void listTables(HBaseAdmin admin) throws IOException {
        System.out.println("列举表");
        List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
        for (TableDescriptor tableDescriptor : tableDescriptors) {
            System.out.println(
                    tableDescriptor.getTableName().getNameAsString()
            );
        }
    }

    /**
     * 创建NS
     *
     * @param admin 管理员对象
     * @throws IOException IO异常
     */
    public void createNs(HBaseAdmin admin) throws IOException {
        NamespaceDescriptor descriptor = NamespaceDescriptor.create("nstest").build();
        admin.createNamespace(descriptor);
    }

    /**
     * 删除NameSpace
     *
     * @param admin 管理员对象
     * @throws IOException IO异常
     */
    public void dropNs(HBaseAdmin admin) throws IOException {
        System.out.println("删除namespace");
        //ERROR:Only empty namespaces can be removed. Namespace ns1 has 1 tables
        //admin.deleteNamespace("ns1");
        admin.deleteNamespace("nstest");
    }

    /**
     * 列举所有namespace
     *
     * @param admin 管理员对象
     * @throws IOException IO异常
     */
    public void listAllNs(HBaseAdmin admin) throws IOException {
        System.out.println("列举所有namespace");
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println(namespaceDescriptor.getName());
        }

    }


    public HBaseAdmin getAdmin(Connection conn) throws IOException {
        return (HBaseAdmin) conn.getAdmin();
    }
}
