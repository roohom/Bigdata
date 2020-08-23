import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * @ClassName: HDFSJavaClientTest
 * @Author: Roohom
 * @Function: HDFS的JavaAPI测试
 * @Date: 2020/8/21 14:35
 * @Software: IntelliJ IDEA
 */
public class HDFSJavaClientTest {

    /**
     * 构建一个Configuration对象
     * 功能：用于管理当前Hadoop中的所有属性配置对象，所有Hadoop程序都需要这个对象
     * 1-先加载所有*-default.xml【Jar包中自带】，加载所有默认配置
     * 2-然后加载用户自定义配置*-site.xml文件，替换默认属性值
     * 配置：让IDEA中代码客户端读取到HDFS地址
     * 方式一：将Linux中core-site.xml文件放入项目的环境变量中，放入resources目录中即可
     * 方式二：直接在Configuration中进行设置
     */
    public FileSystem getHDFSConnect() {
        //以方式二直接在conf对象中指定HDFS地址，再进行连接
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.88.221:8020");
        FileSystem fs = null;
        try {
            //构建一个HDFS连接对象
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.88.221:8020"), conf);
        //构建方式三:指定以某种身份访问HDFS
//        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.88.221:8020"), conf,"root");
//        System.out.println(fs);
        return fs;
    }

    /**
     * 打印整个集群所有DN的信息
     *
     * @throws IOException
     */
    @Test
    public void reportHDFSCluster() throws IOException {
        //强转为一个分布式文件系统
        DistributedFileSystem hdfs = (DistributedFileSystem) getHDFSConnect();
        //获取集群的信息
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

        //迭代打印
        for (DatanodeInfo dataNodeStat : dataNodeStats) {
            System.out.println(dataNodeStat.getDatanodeReport());
        }
    }

    /**
     * 在HDFS创建目录
     *
     * @throws IOException
     */
    @Test
    public void createDir() throws IOException {
        FileSystem fs = getHDFSConnect();
        //构建要创建的路径
        Path path = new Path("/windows");
        //先判断要创建的目录是否已经存在
        if (fs.exists(path)) {
            //已经存在则删除该目录
            fs.delete(path, true);
        }
        //创建目录
        fs.mkdirs(path);
        //释放
        fs.close();
    }

    /**
     * 列举HDFS
     *
     * @throws IOException
     */
    @Test
    public void listHDFS() throws IOException {
        FileSystem fs = getHDFSConnect();
        Path path = new Path("/");

        //方式一:
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath());
        }

        //方式二:
//        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(path, true);
//        while (listFiles.hasNext()) {
//            LocatedFileStatus next = listFiles.next();
//            System.out.println(next.getPath());
//        }
        //释放
        fs.close();
    }

    /**
     * 文件上传
     *
     * @throws IOException
     */
    @Test
    public void uploadHDFS() throws IOException {
        FileSystem fs = getHDFSConnect();
        //构建本地路径
        Path localPath = new Path("file:///E:\\IDEAJ\\Bigdata\\datas\\lianjia\\secondhouse.csv");
        Path path = new Path("/wordcount/input/");

        fs.copyFromLocalFile(localPath, path);
        fs.close();
    }


    /**
     * 文件下载
     *
     * @throws IOException
     */
    @Test
    public void downloadHDFS() throws IOException {
        FileSystem fs = getHDFSConnect();
        Path path = new Path("/wordcount/input/SogouQ.reduced");
        Path localPath = new Path("file:///D:\\input");

        fs.copyToLocalFile(path, localPath);

        fs.close();
    }


    /**
     * 实现将文件合并之后上传至HDFS
     *
     * @throws IOException IO异常
     */
    @Test
    public void mergeUpload() throws IOException {
        FileSystem hdfs = getHDFSConnect();
        //在hdfs上构建一个文件
        Path path = new Path("/windows/merge.txt");
        FSDataOutputStream outputStream = hdfs.create(path);
        //构建输入流
        //构建本地文件系统
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        Path inputPath = new Path("D:\\input");
        FileStatus[] fileStatuses = local.listStatus(inputPath);
        //迭代每个文件，构建每个文件的输入流
        for (FileStatus fileStatus : fileStatuses) {
            //打开文件
            FSDataInputStream input = local.open(fileStatus.getPath());
            //将输入流的数据写入输出流
            IOUtils.copyBytes(input, outputStream, 4096);
            //关闭流
            input.close();
        }
        outputStream.close();
        local.close();
        hdfs.close();
    }
}
