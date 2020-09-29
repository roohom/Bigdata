package ESHbaseIntergration;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: ExcelUtil
 * @Author: Roohom
 * @Function: Excel工具类
 * @Date: 2020/9/29 16:22
 * @Software: IntelliJ IDEA
 */
public class ExcelUtil {
    /**
     * Excel解析函数，用于解析Excel
     *
     * @param path Excel文件地址
     * @return 封装了Java Bean的集合
     * @throws IOException 读取Excel的异常
     */
    public static List<EsArticle> parseExcel(String path) throws IOException {
        //构建返回值对象
        List<EsArticle> esAarticles = new ArrayList<>();
        //构建Excel的解析对象
        FileInputStream inputStream = new FileInputStream(path);
        XSSFWorkbook sheets = new XSSFWorkbook(inputStream);
        //获取excel文件的第一个sheet
        XSSFSheet sheet = sheets.getSheetAt(0);
        int lastRowNum = sheet.getLastRowNum();
        for (int i = 1; i < lastRowNum; i++) {
            XSSFRow row = sheet.getRow(i);
            String id = row.getCell(0).toString();
            String title = row.getCell(1).toString();
            String from = row.getCell(2).toString();
            String time = row.getCell(3).toString();
            String readCount = row.getCell(4).toString();
            String content = row.getCell(5).toString();
            //构建Java Bean
            EsArticle esArticle = new EsArticle(id, title, from, time, readCount, content);
            //将自定义的 java bean 装入List
            esAarticles.add(esArticle);
        }
        return esAarticles;
    }
}
