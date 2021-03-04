package flinklearning._1source;

import flinklearning._1source.model.TraceSegmentRecordInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 实现自定实现 Mysql 数据读取器
 * 支持 定点读取和至少一次
 */
public class E05ReadMysqlRichSource02 extends RichSourceFunction<TraceSegmentRecordInfo> implements CheckpointedFunction {
    //常量名称
    public static final String url = "url";
    public static final String username = "username";
    public static final String password = "password";
    //数据库连接资源对象
    private transient Connection connection;
    private transient PreparedStatement statement;
    //查询SQL
    private String querySql;
    //是否继续运行
    private volatile boolean isRunning = true;

    //查询开始时间
    private volatile LocalDateTime startDateTime;
    //时间增长步长  单位分钟
    private volatile int timeStep = 1;


    /**
     * 存储 state 的变量.
     */
    private ListState<LocalDateTime> startDateTimeState;


    public E05ReadMysqlRichSource02(String querySql, LocalDateTime startDateTime) {
        super();
        //设置查询SQL
        this.querySql = querySql;
        //初始化查询其实时间
        this.startDateTime = startDateTime;

    }

    /**
     * 在run方法前执行
     * 进行资源初始化
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //获取系统配置
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        //创建连接资源
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(parameterTool.get(url), parameterTool.get(username), parameterTool.get(password));
        statement = connection.prepareStatement(querySql);
    }

    /**
     * 资源进行关闭 在线程中断前执行
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        System.err.println("第五执行 close---------------");
        isRunning = false;
        //关闭资源连接
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }


    }

    /**
     * 在支持只支持不会并行执行
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<TraceSegmentRecordInfo> ctx) throws Exception {
        final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        //获取检查点锁
        final Object lock = ctx.getCheckpointLock();
        //循环遍历
        while (isRunning) {

            //按照 时间步长生成  查询起止时间
            String startDateTimeStr = dtf.format(startDateTime);
            LocalDateTime endDateTime = startDateTime.plusMinutes(timeStep);
            if (endDateTime.isAfter(LocalDateTime.now())) {
                //最新截止时间 大于系统时间休眠后跳出循环
                Thread.sleep(timeStep * 60000);
                continue;
            }
            String endDateTimeStr = dtf.format(endDateTime);
            //设置参数
            statement.setString(1, startDateTimeStr);
            statement.setString(2, endDateTimeStr);

            //查询数据且封装参数
            List<TraceSegmentRecordInfo> containerList = new ArrayList<>();
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
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
                containerList.add(info);
            }
            System.out.println(String.format("执行结果[%s - %s] 查询条数%s", startDateTimeStr, endDateTimeStr, containerList.size()));
            if (CollectionUtils.isNotEmpty(containerList)) {
                for (Iterator<TraceSegmentRecordInfo> iterator = containerList.iterator(); iterator.hasNext(); ) {
                    //发送数据
                    ctx.collect(iterator.next());
                }
            }

            synchronized (lock) {
                //推送完毕 最后使用endDateTime 设置为起始时间
                startDateTime = endDateTime;
            }
        }
    }

    @Override
    public void cancel() {
        System.err.println("取消时候执行 cancel---------------");
        //设置停止run方法循环
        isRunning = false;

    }

    /**
     * 检查点快照恢复
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.err.println("快照时候执行 snapshotState---------------");
        //快照最新状态
        startDateTimeState.clear();
        startDateTimeState.add(startDateTime);
        System.out.println("快照结果: " + startDateTime);
    }

    /**
     * 启动时执行快照恢复
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.err.println("第二执行 initializeState---------------");
        //定义状态描述
        ListStateDescriptor<LocalDateTime> startDateTimeStateDescriptor = new ListStateDescriptor<LocalDateTime>("startDateTimeState", TypeInformation.of(LocalDateTime.class));
        startDateTimeState = context.getOperatorStateStore().getListState(startDateTimeStateDescriptor);
        //从状态中恢复，
        if (context.isRestored()) {
            //只有重状态恢复 才会进来，正常启动 isRestored  = false
            for (LocalDateTime localDateTime : startDateTimeState.get()) {
                startDateTime = localDateTime;

            }
            System.out.println("恢复结果:" + startDateTime);
        }
    }
}
