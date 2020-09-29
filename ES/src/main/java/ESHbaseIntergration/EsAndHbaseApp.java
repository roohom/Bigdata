package ESHbaseIntergration;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

/**
 * @ClassName: EsAndHbaseApp
 * @Author: Roohom
 * @Function: 用于实现ES构建Hbase的二级索引
 * @Date: 2020/9/29 16:16
 * @Software: IntelliJ IDEA
 */
public class EsAndHbaseApp {
    static String tbName = "articles";
    static String family = "article";

    public static void main(String[] args) throws IOException, InterruptedException {
        //TODO:1 解析Excel
        String path = "datas\\Excel\\hbaseEs.xlsx";
        //返回的每条数据的集合
        List<EsArticle> esArticles = ExcelUtil.parseExcel(path);
        //迭代打印每条数据
        System.out.println("*************解析成功...打印如下:***********");
        for (EsArticle esArticle : esArticles) {
            System.out.println(esArticle);
        }

        //TODO:2 将解析好的数据写入Hbase和ES
//        System.out.println("*************    正在写入ES     ***********");
//        ESUtil.write(esArticles);
//        System.out.println("*************    写入ES成功     ***********");
//        System.out.println("*************   正在写入HBase   ***********");
//        writeHbase(esArticles);
//        System.out.println("*************   写入HBase成功   ***********");
//        Thread.sleep(5);

        //TODO:3 检索:给定标题的关键词
        System.out.println("****          请输入你想查找的内容:      ******");
        System.out.println("***************   查询结果如下:  ************");
        search(new Scanner(System.in).next());

    }

    /**
     * 检索 传入关键词至ES获取ID，ID对应于HBase中的Rowkey，利用rowkey查询Hbase中的数据
     *
     * @param keyWord 所要查询的关键词
     * @throws IOException 可能发生的读写异常
     */
    private static void search(String keyWord) throws IOException {
        List<EsArticle> esArticles = ESUtil.read(keyWord);
        for (EsArticle esArticle : esArticles) {
            String rowkey = esArticle.getId();
            String content = HbaseUtil.read(tbName, rowkey, family, "content");
            System.out.println(content);
        }
    }

    /**
     * 将自定义java bean类型的数据写入hbase
     *
     * @param esArticles 自定义java bean
     * @throws IOException 可能出线的写入异常
     */
    public static void writeHbase(List<EsArticle> esArticles) throws IOException {
        for (EsArticle esArticle : esArticles) {
            HbaseUtil.write(tbName, esArticle.getId(), family, "title", esArticle.getTitle());
            HbaseUtil.write(tbName, esArticle.getId(), family, "time", esArticle.getTime());
            HbaseUtil.write(tbName, esArticle.getId(), family, "from", esArticle.getFrom());
            HbaseUtil.write(tbName, esArticle.getId(), family, "readCount", esArticle.getReadCount());
            HbaseUtil.write(tbName, esArticle.getId(), family, "content", esArticle.getContent());
        }
    }
}
