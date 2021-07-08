package flinklearning._06cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;

public class BinlogPullTest {
    public static void main(String[] args) throws Exception {
        //创建Source 里面封装了debezium客户端
        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("10.130.9.27")    //mysql地址
                .port(3306)                 //mysql 端口
                .databaseList("cdc_test")   //监听的数据库
//                .tableList("cdc_test.person") //监听的表  不填写就监听整个库的表
                .username("flink_cdc")              //用户名
                .password("123456")                 //用户密码
                .serverId(4447)                     //连接服务端的ID 可以不填写 每次连接就生成一个新的
                .serverTimeZone("Asia/Shanghai")    //解析时间的时区
                .startupOptions(StartupOptions.initial())  //启动模式  initial 快照模式
//                .deserializer(new StringDebeziumDeserializationSchema()) // 将debezium原生数据格式转换为字符String
                .deserializer(new JsonDebeziumDeserializationSchema()) // 自定义json解析器
                .build();

        //flink环境执行器
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //检查点设置
        env.enableCheckpointing(1000 * 60 * 10);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000 * 60 * 10);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);
        //从Source源接收数据
        DataStreamSource<String> source = env.addSource(sourceFunction);
        //打印数据
        source.print().setParallelism(1);
        //启动任务
        env.execute("Binlog Cdc Job");

        HashMap a = new HashMap();

    }
}
