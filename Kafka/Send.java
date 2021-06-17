package Kafka;

import org.apache.kafka.clients.producer.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class Send {

    public static void main(String[] args){
        Properties Prop = new Properties();
        Prop.put("bootstrap.servers", "server1:9092");//服务器的broker，一般可以多个
        Prop.put("acks", "all");//所有follower都响应了才认为消息提交成功，即"committed"
        Prop.put("retries", 1);//重试次数
        Prop.put("batch.size", 16384);//批次大小
        Prop.put("linger.ms", 1);//等待时间
        Prop.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        Prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        /*
         *  第一个参数是 ProducerRecord 类型的对象，封装了目标 Topic，消息的 kv
         *  第二个参数是一个 CallBack 对象，当生产者接收到 Kafka 发来的 ACK 确认消息的时候，
         *  会调用此 CallBack 对象的 onCompletion() 方法，实现回调功能
         */
        try {
            KafkaProducer<String, String> producer = new KafkaProducer<>(Prop);//kafka生产者
            String topic = "myTopic";
            String topic1 = "test";
            long s=0;

            for (int i = 0; i < 100; i++) {

                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
                String startTime = df.format(new Date());
//                s = pare(startTime) ;
                long s1 = pare(startTime) + 10;
                String key = (i + 10)+"";
                String value = (i * 10) + "";

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i));
                ProducerRecord<String, String> producerRecord1 = new ProducerRecord<>(topic1, key, value);

                producer.send(producerRecord, new DemoCallBack(s, i, Integer.toString(i)));
                producer.send(producerRecord1, new DemoCallBack(s1, i + 10, value));
                System.out.println(i + " : " + i);
                System.out.println(key+ " : " + value);
                Thread.sleep(1000);
            }
//            producer.send(new ProducerRecord<>(topic, messageNo, messageStr),
//                    new DemoCallBack(startTime, messageNo, messageStr));
//        }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static String get(){
        Date d=new Date();

        SimpleDateFormat sim=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        String time=sim.format(d);

        System.out.println(time);

        return time;

    }
    public static long pare(String time){
        SimpleDateFormat sim=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        long s=0;

        try {
            s=sim.parse(time).getTime();

        } catch (ParseException e) {
        // TODO Auto-generated catch block

            e.printStackTrace();

        }
        return s;

    }

}

class DemoCallBack implements Callback {
    /* 开始发送消息的时间戳 */
    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * 生产者成功发送消息，收到 Kafka 服务端发来的 ACK 确认消息后，会调用此回调函数
     * @param metadata 生产者发送的消息的元数据，如果发送过程中出现异常，此参数为 null
     * @param exception 发送过程中出现的异常，如果发送成功为 null
     */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.printf("message: (%d, %s) send to partition %d, offset: %d, in %d\n",
                    key, message, metadata.partition(), metadata.offset(), elapsedTime);
        } else {
            exception.printStackTrace();
        }
    }
}
