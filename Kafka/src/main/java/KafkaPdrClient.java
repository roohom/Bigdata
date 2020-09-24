import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassName: KafkaPdrClient
 * @Author: Roohom
 * @Function: 生产者客户端
 * @Date: 2020/9/24 20:51
 * @Software: IntelliJ IDEA
 */
public class KafkaPdrClient {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","node1:9092,node2:9092,node3:9092");
        props.put("acks","all");
        props.put("retries",0);
        props.put("batch.size",16);
        props.put("linger.ms",1);
        props.put("buffer.memory",33445532);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            //指定Key,就按照Key的hash取余
            producer.send(new ProducerRecord<String, String>("bigdata",Integer.toString(i),"CodeForFree"+i));
            //没有指定key，就轮询
//            producer.send(new ProducerRecord<String, String>("bigdata","CodeForFree"+i))
            //指定分区，同时也需要指定Key和Value
//            producer.send(new ProducerRecord<String, String>("bigdata",0,Integer.toString(i),"CodeForFree"+i));
        }
        //不要忘记关闭客户端，经试验，不关闭，消费者接收不到，这根据你的batch.size来决定，默认batch满了就会发送，如果没满就不发送，直到你调用close()
        producer.close();
    }
}
