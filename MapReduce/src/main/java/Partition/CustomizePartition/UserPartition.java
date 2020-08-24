package Partition.CustomizePartition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName: UserPartition
 * @Author: Roohom
 * @Function: 自定义分区器
 * @Date: 2020/8/24 12:22
 * @Software: IntelliJ IDEA
 */
public class UserPartition extends Partitioner<Text, IntWritable> {
    String houseSource = "浦东";
    /**
     * Map阶段输出的每一条K2V2都会调用一次这个方法用于标记会被哪个reduce处理
     *
     * @param key         当前的K2
     * @param intWritable 当前的V2
     * @param i           是第几个Reduce
     * @return 是否为浦东的标记
     */
    @Override
    public int getPartition(Text key, IntWritable intWritable, int i) {
        String region = key.toString();
        if (houseSource.equals(region)) {
            return 0;
        } else {
            //不是浦东的返回1
            return 1;
        }
    }
}
