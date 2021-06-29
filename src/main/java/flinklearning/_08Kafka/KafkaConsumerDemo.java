package flinklearning._08Kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerDemo {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.130.11.208:9092,10.130.10.243:9092,10.130.11.207:9092");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "one_test_group");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //关闭自动提交 默认是开启5s一次 auto.commit.interval.ms 控制
//        prop.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024"); //最少拉取字节数
//        prop.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "5"); //消费者读取时最长等待时间
//        prop.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "5"); //每个分区返回的最多字节数，默认为1M如果小于max.message.size broke最大接收数 会消费不了
//        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //earliest latest 设置醉在或者最新节点消费
//        prop.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor"); //org.apache.kafka.clients.consumer.RangeAssignor 默认范围  org.apache.kafka.clients.consumer.RoundRobinAssignor（使用轮询策略）
        prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500"); //控制一个poll()调用返回的记录数


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Collections.singleton("skywalking-metrics")); //订阅主题 可以多个
        int i = 0;
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(500));
            System.out.println(String.format("当前 %s 拉取Kafka消息数量：%s", ++i, consumerRecords.count()));
            for (Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator(); iterator.hasNext(); ) {
                ConsumerRecord<String, String> record = iterator.next();
                System.out.println(String.format("提交offset parttion = %s - offset= %s",record.partition(), record.offset()));


                if (i == 3) {
                    int t = 5 / 0;
                }


                System.out.println(String.format("数据信息 topic = %s - parttion = %s - offset= %s ", record.topic(), record.partition(), record.offset())); //处理
                try {
                    TimeUnit.SECONDS.sleep(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

            consumer.commitAsync(); // 异步提交

        }
    }
}
