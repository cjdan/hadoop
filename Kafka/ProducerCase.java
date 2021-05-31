package Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerCase {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "server1:9092");//kafka集群，broker-list
        props.put("acks", "all");//所有follower都响应了才认为消息提交成功，即"committed"
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try  {
            producer = new KafkaProducer<>(props);//kafka生产者
            String topic = "myTopic";
            for (int i = 0; i < 100; i++) {
                ProducerRecord<String,String> producerRecord = new ProducerRecord(topic, Integer.toString(i), Integer.toString(i));
                System.out.println(Integer.toString(i)+" : " +Integer.toString(i));
                producer.send(producerRecord);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
