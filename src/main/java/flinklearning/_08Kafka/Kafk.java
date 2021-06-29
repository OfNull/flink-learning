package flinklearning._08Kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

import java.util.Optional;
import java.util.Properties;

public class Kafk {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        //省略部分配置...
//        kafkaProps.put("partitioner.class", "org.apache.kafka.clients.producer.RoundRobinPartitioner");
        FlinkKafkaProducer<String> producer= new FlinkKafkaProducer<String>( "topic", new SimpleStringSchema(), kafkaProps,  Optional.of(new RangeFlinkKafkaPartitioner<String>()));

    }
}
