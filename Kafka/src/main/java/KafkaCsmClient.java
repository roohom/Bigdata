import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName: KafkaCsmClient
 * @Author: Roohom
 * @Function: kafka消费者客户端
 * @Date: 2020/9/25 16:16
 * @Software: IntelliJ IDEA
 */
public class KafkaCsmClient {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        props.put("group.id", "practice");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        ArrayList<String> list = new ArrayList<String>();
        list.add("bigdata");
        consumer.subscribe(list);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                String key = record.key();
                String value = record.value();
                System.out.println(topic + "\t" + partition + "\t" + offset + "\t" + key + "\t" + value);
            }
        }
    }
}
