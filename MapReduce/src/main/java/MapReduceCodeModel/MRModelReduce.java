package MapReduceCodeModel;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: MapReduceCodeModel.MRModelReduce
 * @Author: Roohom
 * @Function: Reduce模板类
 * @Date: 2020/8/22 15:03
 * @Software: IntelliJ IDEA
 */


/**
 * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *        输入的KV类型：由Map输出的类型决定
 *             K：K2类型
 *             V：V2类型
 *        输出的KV类型：由reduce方法决定
 *             WordCount为例：每个单词出现的 次数
 *             K：Text，单词
 *             V：IntWritable，单词出现的次数
 */

public class MRModelReduce extends Reducer<Text, IntWritable,Text, IntWritable> {
    /**
     *shuffle以后每组的会调用一次reduce方法
     * @param key K2
     * @param values 相同的K2的所有V2放入一个迭代器中
     * @param context 上下文对象，用于传递数据
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       //处理逻辑由需求来定义
    }
}
