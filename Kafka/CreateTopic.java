package Kafka;




import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CreateTopic {
    public static <CreateTopicsResult> void main(String[] args) throws ExecutionException, InterruptedException {
        Properties Prop = new Properties();
        Prop.put("bootstrap.servers", "server1:9092");
        AdminClient adminClient = AdminClient.create(Prop);
        NewTopic newTopic = new NewTopic("mytopic1",1,(short)1);
        NewTopic newTopic1 = new NewTopic("mytopic2",2,(short)2);
        ArrayList<NewTopic> topics = new ArrayList<>();
        topics.add(newTopic);
        topics.add(newTopic1);
        org.apache.kafka.clients.admin.CreateTopicsResult result = adminClient.createTopics(topics);
        try {
            result.all().get();
        }catch (Exception e ){
            e.printStackTrace();
        }


    }
}
