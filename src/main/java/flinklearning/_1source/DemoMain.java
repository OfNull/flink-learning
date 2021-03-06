package flinklearning._1source;

import flinklearning._1source.model.TraceSegmentRecordInfo;
import flinklearning._4config.ParameterToolEnvironmentUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public class DemoMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //自己实现带环境的系统参数解析
        ParameterTool parameterTool = ParameterToolEnvironmentUtils.createParameterTool(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        //开启检查点
        env.enableCheckpointing(2000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置文件系统作为状态后端
        FsStateBackend fsStateBackend = new FsStateBackend("file:///usr/flink-1.12.1/tmp");
        env.setStateBackend(fsStateBackend);
        // 执行结果[2021-02-09 07:39:01 - 2021-02-09 07:40:01] 查询条数177
        String sql = "SELECT * from trace_segment_record where create_time >= ? AND create_time < ?";
        //source
        DataStreamSource<TraceSegmentRecordInfo> source2 = env.addSource(new E05ReadMysqlRichSource02(sql, LocalDateTime.of(2021, 01, 8, 0, 0, 01))).setParallelism(2);

        //过滤
        SingleOutputStreamOperator<TraceSegmentRecordInfo> filter = source2.filter(new FilterFunction<TraceSegmentRecordInfo>() {
            @Override
            public boolean filter(TraceSegmentRecordInfo value) throws Exception {
                //过滤等于null的数据
                return value.getEndpointName() != null;
            }
        });

        //sink
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder().withBatchIntervalMs(2000L).withBatchSize(5000).build();
        JdbcConnectionOptions connectionOptions = (new JdbcConnectionOptions.JdbcConnectionOptionsBuilder())
                .withUrl("jdbc:mysql://10.130.9.27:3306/yto_data_pipeline_init?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("test")
                .withPassword("s1iAY4Qz").build();
        final String sinkSql = "REPLACE INTO trace_segment_record (segment_id, trace_id, service_name, service_ip, endpoint_name, start_time, end_time,latency,is_error, data_binary,time_bucket,create_time,statement) VALUE(?,?,?,?,?,?,?,?,?,?,?,?,?)";

        filter.addSink(JdbcSink.sink(sinkSql, (ps, v) -> {
            ps.setString(1, v.getSegmentId());
            ps.setString(2, v.getTraceId());
            ps.setString(3, v.getServiceName());
            ps.setString(4, v.getServiceIp());
            ps.setString(5, v.getEndpointName());
            ps.setLong(6, v.getStartTime());
            ps.setLong(7, v.getEndTime());
            ps.setLong(8, v.getLatency());
            ps.setInt(9, v.getIsError());
            ps.setString(10, v.getDataBinary());
            ps.setLong(11, v.getTimeBucket());
            ps.setTimestamp(12, new Timestamp(v.getCreateDate().getTime()));
            ps.setString(13, v.getStatement());
        }, executionOptions, connectionOptions)).uid("sink_mysql");

        env.execute("aaa");
    }
}
