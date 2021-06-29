package flinklearning._08Kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaProducerDemo {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.130.11.208:9092,10.130.10.243:9092,10.130.11.207:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(prop);

        String[] names = {"周坤", "李白", "杜甫", "张若虚", "欧阳修","白居易"};
        int[] ages = {18, 22, 54, 32, 99, 18};

        while (true) {
            Random random = new Random();
            int nextInt = random.nextInt(6);
            Map<String, Object> map = new HashMap<>(5);
            map.put("name", names[nextInt]);
            map.put("age", ages[nextInt]);
            map.put("ts", System.currentTimeMillis());

            String jsonString = JSON.toJSONString(map);
            System.out.println(jsonString);

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ProducerRecord record = new ProducerRecord("hive-topic", jsonString);
            Future send = kafkaProducer.send(record, (RecordMetadata data, Exception e)->{
                System.out.println(data.topic()+":"+data.partition()+":"+data.offset());
            });

        }

    }
}
