package flinklearning._06cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Description TODO
 * @Date 2021/3/22 7:14 下午
 * @Created by zhoukun
 */
public class _01MysqlCdcTest {
    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("172.16.62.1")
                .port(3306)
                .databaseList("cdc") // monitor all tables under inventory database
                .tableList("student")
                .username("root")
                .password("123456")
                .serverId(4444)
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(sourceFunction).print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Mysql Cdc Job");
    }
}
