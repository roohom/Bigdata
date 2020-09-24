package CodeModel;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassName: CodeModel.KafkaProducerClient
 * @Author: Roohom
 * @Function: Kafka生产者客户端
 * @Date: 2020/9/24 19:54
 * @Software: IntelliJ IDEA
 */
public class KafkaProducerClient {
    public static void main(String[] args) {
        //构建properties对象
        Properties props = new Properties();
        //指定连接的kafka地址
        props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        /**
         * 指定生产者给Kafka发送消息的模式
         * 0-生产者不管kafka是否收到，都直接发送下一条，快，但是数据易丢失
         * 1-生产者发送一条数据到Topic的分区中，只要写入了leader分区，就返回一个ack给生产者，继续发送下一条
         * all-生产者发送一条数据到Topic的分区中，Topic必须保证所有分区副本都同步成功了 ，继续发送下一条，最安全，最慢
         */
        props.put("acks", "all");
        //如果发送失败，重试次数
        props.put("retries", 0);
        //每次从缓存中发送的批次的大小
        props.put("batch.size", 16384);
        //间隔时间 milliseconds
        props.put("linger.ms", 1);
        //生产数据的缓存
        props.put("buffer.memroy", 33445532);
        //序列化机制：kafka是以KV形式进行数据存储，K可以没有，写入的数据是Value
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        /**
         * 构建循环，模拟不断产生新的数据
         * 分区规则
         *  方式一：如果指定了Key，默认按照Key的hash取余分区个数
         *  方式二：如果没有Key，按照轮询
         *  方式三：如果指定了分区，只写入对应的分区
         *  方式四：自定义分区
         */
        for (int i = 0; i < 10; i++) {
            //生产者对象，调用send方法往Topic中放入数据
            //方式一:ProducerRecord<String, String>(TopicName,K,V)
//            producer.send(new ProducerRecord<String, String>("bigdata", Integer.toString(i), "itcast" + i));
            //方式二:指定Topic和Value，没有KEY
//            producer.send(new ProducerRecord<String, String>("bigdata","itcast"+i));
            //方式三：指定写入一个分区
            producer.send(new ProducerRecord<String, String>("bigdata", 0, Integer.toString(i), "itcast" + i));
        }
        //关闭生产者，不关闭消费者收不到(batch没满的情况下)
        producer.close();
    }
}
