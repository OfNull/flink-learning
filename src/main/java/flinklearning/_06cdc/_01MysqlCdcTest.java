package flinklearning._06cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.formats.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Properties;

/**
 * @Description TODO
 * @Date 2021/3/22 7:14 下午
 * @Created by zhoukun
 */
public class _01MysqlCdcTest {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("debezium.snapshot.locking.mode", "none");

        //
        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("10.130.36.244")
                .port(3306)
                .databaseList("monitor-uat") // monitor all tables under inventory database
                .tableList("monitor-uat.monitor_heart_record")
                .username("user")
                .password("123456")
                .serverId(4447)
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(properties)
//                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to String
//                .deserializer(new DebeziumJsonDeserializationSchema(new RowType(), null, Types.POJO(RowData.class), false, true, TimestampFormat.SQL)) // converts SourceRecord to String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 60 * 10);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000 * 60 * 10);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);
        DataStreamSource<String> source = env.addSource(sourceFunction);
        source.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String value) throws Exception {
//                System.out.println("----------------" + value);
                return null;
            }
        });
        source.print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Mysql Cdc Job");
    }
}
