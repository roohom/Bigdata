package MapReduceCodeModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: MPModelMapper
 * @Author: Roohom
 * @Function: MapReduce编程模板的Mapper类
 * @Date: 2020/8/22 14:55
 * @Software: IntelliJ IDEA
 */

/**
 * Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 *     输入的KV类型:由输入类决定
 *     TextInoutFormat:
 *          K:LongWritable，行的偏移量
 *          V:Text，行的内容
 *     输出的KV类型:由map方法决定，由处理逻辑决定
 *     WordCount程序中:
 *          K:Text，单词
 *          V:InterWritable，恒为1
 */

public class MRModelMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    /**
     * 所有的input传递的KV都会调用一次map方法
     * @param key 当前的K1
     * @param value 当前的V1
     * @param context 上下文对象，用于获取数据，向下传递数据
     * @throws IOException IO异常
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //自定义处理数据
    }
}
