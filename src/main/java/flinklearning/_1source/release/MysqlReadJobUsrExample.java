package flinklearning._1source.release;

import flinklearning._1source.model.TraceSegmentRecordInfo;
import flinklearning._4config.ParameterToolEnvironmentUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * 使用案例
 */
public class MysqlReadJobUsrExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //自己实现带环境的系统参数解
        ParameterTool parameterTool = ParameterToolEnvironmentUtils.createParameterTool(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        //开启检查点
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置文件系统作为状态后端
        FsStateBackend fsStateBackend = new FsStateBackend("file:///usr/flink-1.12.1/tmp");
        env.setStateBackend(fsStateBackend);


        //source
        JdbcReadExecutionOptions readExecutionOptions = JdbcReadExecutionOptions.builder().withQuerySql("SELECT * from trace_segment_record where create_time >= ? AND create_time < ?")
                .withStartDateTime(LocalDateTime.of(2021, 01, 8, 0, 0, 01))
                .withTimeStep(1).build();
        JdbcConnectionOptions readConnectionOptions = (new JdbcConnectionOptions.JdbcConnectionOptionsBuilder())
                .withUrl("jdbc:mysql://10.130.36.244:3306/monitor-uat?useUnicode=true&characterEncoding=UTF-8&useSSL=false&autoReconnect=true&&serverTimezone=Asia/Shanghai")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("tech789").build();
        JdbcReadRichSource<TraceSegmentRecordInfo> jdbcReadRichSource = new JdbcReadRichSource<>(readConnectionOptions, readExecutionOptions,
                resultSet -> {
                    TraceSegmentRecordInfo info = new TraceSegmentRecordInfo();
                    info.setSegmentId(resultSet.getString("segment_id"));
                    info.setTraceId(resultSet.getString("trace_id"));
                    info.setServiceName(resultSet.getString("service_name"));
                    info.setServiceIp(resultSet.getString("service_ip"));
                    info.setEndpointName(resultSet.getString("endpoint_name"));
                    info.setDataBinary(resultSet.getString("data_binary"));
                    info.setTimeBucket(resultSet.getLong("time_bucket"));
                    info.setStartTime(resultSet.getLong("start_time"));
                    info.setEndTime(resultSet.getLong("end_time"));
                    info.setLatency(resultSet.getInt("latency"));
                    info.setIsError(resultSet.getInt("is_error"));
                    info.setCreateDate(resultSet.getDate("create_time"));
                    info.setStatement(resultSet.getString("statement"));
                    return info;
                });
        SingleOutputStreamOperator<TraceSegmentRecordInfo> source1 = env.addSource(jdbcReadRichSource).returns(TraceSegmentRecordInfo.class);

        //sink
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder().withBatchIntervalMs(2000L).withBatchSize(5000).build();
        JdbcConnectionOptions connectionOptions = (new JdbcConnectionOptions.JdbcConnectionOptionsBuilder())
                .withUrl("jdbc:mysql://10.130.9.27:3306/yto_data_pipeline_init?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("test")
                .withPassword("s1iAY4Qz").build();
        final String sinkSql = "REPLACE INTO trace_segment_record (segment_id, trace_id, service_name, service_ip, endpoint_name, start_time, end_time,latency,is_error, data_binary,time_bucket,create_time,statement) VALUE(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        source1.addSink(JdbcSink.sink(sinkSql, (ps, v) -> {
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

        env.execute("ETL-MYSQL-JOB");

    }
}
