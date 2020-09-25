import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassName: KafkaPdrCli
 * @Author: Roohom
 * @Function: kafka生产者客户端
 * @Date: 2020/9/25 16:47
 * @Software: IntelliJ IDEA
 */
public class KafkaPdrCli {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","node1:9092,node2:9092,node3:9092");
        props.put("acks","all");
        props.put("linger.ms",1);
        props.put("retries",0);
        props.put("batch.size",16384);
        props.put("buffer.memory",33445532);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("bigdata",Integer.toString(i),"CodeForNikonCammera"+i));
//            producer.send(new ProducerRecord<String, String>("bigdata","CodeForNikonCammera"+i));
//            producer.send(new ProducerRecord<String, String>("bigdata",0,Integer.toString(i),"CodeForNikonCammera"+i));
        }
        producer.close();
    }
}
