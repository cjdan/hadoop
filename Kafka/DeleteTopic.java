package Kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.Properties;

public class DeleteTopic {
    public static void main(String[] args) {
        Properties Prop = new Properties();
        Prop.put("bootstrap.servers", "server1:9092");
        AdminClient adminClient = AdminClient.create(Prop);
        String newTopic = "mytopic";
//        String newTopic1 = "mytopic2";
        ArrayList<String> topics = new ArrayList<>();
        topics.add(newTopic);
//        topics.add(newTopic1);
        DeleteTopicsResult result = adminClient.deleteTopics(topics);
        try {
            result.all().get();
        }catch (Exception e ){
            e.printStackTrace();
        }

    }
}
