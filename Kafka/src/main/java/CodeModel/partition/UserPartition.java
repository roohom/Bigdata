package CodeModel.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Random;

/**
 * @ClassName: UserPartition
 * @Author: Roohom
 * @Function: kafka自定义分区类
 * @Date: 2020/9/26 10:16
 * @Software: IntelliJ IDEA
 */
public class UserPartition implements Partitioner {
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取分区个数
        Integer countForTopic = cluster.partitionCountForTopic(topic);
        Random random = new Random();
        return random.nextInt(countForTopic);
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
