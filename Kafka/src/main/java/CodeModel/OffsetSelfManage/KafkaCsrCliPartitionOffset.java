package CodeModel.OffsetSelfManage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @ClassName: CodeModel.KafkaConsumerClient
 * @Author: Roohom
 * @Function: Kafka消费者客户端, 手动提交offset，并逐个分区提交offset
 * @Date: 2020/9/24 20:12
 * @Software: IntelliJ IDEA
 */
public class KafkaCsrCliPartitionOffset {
    public static void main(String[] args) {
        //构建properties用于管理配置
        Properties props = new Properties();
        //指定kafka的服务器地址
        props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        //指定消费者的组id
        props.put("group.id", "test");
        //是否让kafka自动提交消费的偏移量
        props.put("enable.auto.commit", "false");
        //反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //构建一个kafka消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //订阅要消费的Topic
        consumer.subscribe(Arrays.asList("bigdata"));
        //指定分区消费
//        consumer.assign(Arrays.asList(new TopicPartition("bigdata01",0),new TopicPartition("bigdata01",1)));
        //死循环，一直消费
        while (true) {
            long offset = 0;
            //消费者拉取对应的Topic的数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            //将所有的数据按照分区拆分，对每个分区的数据进行处理
            Set<TopicPartition> partitionSet = records.partitions();
            //迭代分区
            for (TopicPartition partition : partitionSet) {
                //拿到每个分区的信息,根据分区的信息到record中拿到属于这个分区的数据
                List<ConsumerRecord<String, String>> partRecordList = records.records(partition);
                //对这个分区的数据进行处理
                //迭代取出
                for (ConsumerRecord<String, String> record : partRecordList) {
                    //获取这条数据属于哪个Topic
                    String topic = record.topic();
                    //获取这条数据属于这个Topic的哪个分区
                    int part = record.partition();
                    //获取这条数据在这个分区中对应的便宜量
                    offset = record.offset();
                    //获取KEY
                    String key = record.key();
                    //获取VALUE
                    String value = record.value();
                    System.out.println(topic + "\t" + part + "\t" + offset + "\t" + key + "\t" + value);
                }
                //手动提交offset：使用同步提交，处理成功就立即提交，提交成功再获取下一批次
                //partition:要记录的分区的信息，哪个topic的那个分区
                //offsetAndMetadata:当前要来记录的分区的偏移量，本次消费的最高的offset+1
                Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(partition, new OffsetAndMetadata(offset + 1));
                consumer.commitSync(offsets);
            }
        }
    }
}
