package Practice;

import HbaseCodeModel.HbaseDDLTest;
import com.sun.org.apache.bcel.internal.generic.FADD;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName: DDLTest
 * @Author: Roohom
 * @Function: DDL
 * @Date: 2020/10/9 20:09
 * @Software: IntelliJ IDEA
 */
public class DDLTest {
    public static void main(String[] args) throws IOException {
        DDLTest ddlTest = new DDLTest();
        Connection conn = ddlTest.getConnection();
        HBaseAdmin admin = ddlTest.getAdmin(conn);
//        ddlTest.listNamespace(admin);
//        ddlTest.listTables(admin);
//        ddlTest.createNamespace(admin);
//        ddlTest.createTable(admin);
//        ddlTest.createTableOldApi(admin);
//        ddlTest.deleteNamespaceAno(admin);
        ddlTest.deleteTable(admin);
    }


    //获取连接对象
    public Connection getConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        return ConnectionFactory.createConnection(conf);
    }

    //获取管理员对象
    public HBaseAdmin getAdmin(Connection connection) throws IOException {
        return (HBaseAdmin) connection.getAdmin();
    }

    //列举所有的namespace
    public void listNamespace(HBaseAdmin admin) throws IOException {
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println(namespaceDescriptor.getName());
            System.out.println(namespaceDescriptor.getConfiguration());
            System.out.println(namespaceDescriptor.toString());
            System.out.println("======================================");
        }
    }

    //列举所以的表
    public void listTables(HBaseAdmin admin) throws IOException {
        List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
        for (TableDescriptor tableDescriptor : tableDescriptors) {
//            System.out.println(tableDescriptor.getTableName());
            System.out.println(tableDescriptor.getTableName().getNameAsString());
        }
    }

    //创建NameSpace
    public void createNamespace(HBaseAdmin admin) throws IOException {
        NamespaceDescriptor descriptor = NamespaceDescriptor.create("Test").build();
        admin.createNamespace(descriptor);
    }

    //删除NameSpace（无论Namespace下存不存在表都要删除该namespace）
    public void deleteNamespace(HBaseAdmin admin) throws IOException {
        HTableDescriptor[] listTableDescriptorsByNamespace = admin.listTableDescriptorsByNamespace("ns1");
        for (HTableDescriptor hTableDescriptor : listTableDescriptorsByNamespace) {
            if (admin.tableExists(hTableDescriptor.getTableName())) {
                admin.disableTable(hTableDescriptor.getTableName());
                admin.deleteTable(hTableDescriptor.getTableName());
            }
        }
        admin.deleteNamespace("ns1");

    }

    //删除NameSpace（无论Namespace下存不存在表都要删除该namespace）
    public void deleteNamespaceAno(HBaseAdmin admin) throws IOException {
        String namespaceName = "ns3";
        TableName[] tableNames = admin.listTableNamesByNamespace(namespaceName);
        if (tableNames.length == 0) {
            admin.deleteNamespace(namespaceName);
        } else {
            for (TableName tableName : tableNames) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            admin.deleteNamespace(namespaceName);
        }
    }


    //创建表
    public void createTable(HBaseAdmin admin) throws IOException {
        TableName tableName = TableName.valueOf("Test:practice");
        //如果表已经存在就先删除
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        //构建表的构造器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        //构建表的列族构造器
        ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("Info")).build();
        //表的构造器设置列族构造器，添加列族
        tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        //开始构造
        TableDescriptor build = tableDescriptorBuilder.build();
        //创建表
        admin.createTable(build);
    }

    //删除表
    public void deleteTable(HBaseAdmin admin) throws IOException {
        String table = "ns2:stuinfo";
        TableName tableName = TableName.valueOf(table);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    //旧的API创建表
    public void createTableOldApi(HBaseAdmin admin) throws IOException {
        TableName tableName = TableName.valueOf("Test:newCreate");
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes("basic"));
        hTableDescriptor.addFamily(family);
        admin.createTable(hTableDescriptor);
    }
}
