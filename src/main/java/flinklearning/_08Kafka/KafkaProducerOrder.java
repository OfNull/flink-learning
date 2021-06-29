package flinklearning._08Kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class KafkaProducerOrder {

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.130.11.208:9092,10.130.10.243:9092,10.130.11.207:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(prop);

        int[] addressIds = {1, 3, 5, 7, 9, 11};


        while (true) {
            Random random = new Random();
            int i = random.nextInt(10);
            int currentId = i % addressIds.length;


            UserBehavior userBehavior = new UserBehavior();
            userBehavior.setUserId(Math.abs(UUID.randomUUID().getLeastSignificantBits()));
            userBehavior.setItemId(200L);
            userBehavior.setCatId(100L);
            userBehavior.setAction("pv");
            userBehavior.setProvince(addressIds[currentId]);
            userBehavior.setTs(System.currentTimeMillis());

            String jsonString = JSON.toJSONString(userBehavior);
            System.out.println("输出产生结果：" + jsonString);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("user_behavior_topic", jsonString);
            kafkaProducer.send(record);

            try {
                TimeUnit.SECONDS.sleep(i);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
