package flinklearning._1source.release;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
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

/**
 * 通用Jdbc拉去参数
 * 实现最少一次拉取
 *
 * @param <T>
 */
public class JdbcReadRichSource<T> extends RichSourceFunction<T> implements CheckpointedFunction {
    //连接配置对象
    private JdbcConnectionOptions jdbcConnectionOptions;
    //参数解析对象
    private JdbcReadExecutionOptions jdbcReadExecutionOptions;
    //结果解析
    private ResultSetBuilder<T> resultSetBuilder;

    //是否继续运行
    private volatile boolean isRunning = true;
    //查询SQL
    private String querySql;
    //查询开始时间
    private volatile LocalDateTime startDateTime;
    //时间增长步长  单位分钟
    private volatile int timeStep;

    /**
     * 存储 state 的变量.
     */
    private ListState<LocalDateTime> startDateTimeState;


    //数据库连接资源对象
    private transient Connection connection;
    private transient PreparedStatement statement;

    public JdbcReadRichSource(JdbcConnectionOptions jdbcConnectionOptions, JdbcReadExecutionOptions jdbcReadExecutionOptions, ResultSetBuilder<T> resultSetBuilder) {
        this.jdbcConnectionOptions = jdbcConnectionOptions;
        this.jdbcReadExecutionOptions = jdbcReadExecutionOptions;
        this.resultSetBuilder = resultSetBuilder;

        this.startDateTime = jdbcReadExecutionOptions.getStartDateTime();
        this.querySql = jdbcReadExecutionOptions.getQuerySql();
        this.timeStep = jdbcReadExecutionOptions.getTimeStep();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //创建连接资源
        Class.forName(jdbcConnectionOptions.getDriverName());
        connection = DriverManager.getConnection(jdbcConnectionOptions.getDbURL(), jdbcConnectionOptions.getUsername().get(), jdbcConnectionOptions.getPassword().get());
        statement = connection.prepareStatement(querySql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        isRunning = false;
        //关闭资源连接
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }


    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        //获取检查点锁
        final Object lock = ctx.getCheckpointLock();
        final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
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
            int resultSize = 0;
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                T info = resultSetBuilder.apply(resultSet);
                resultSize++;
                ctx.collect(info);
            }
            System.out.println(String.format("执行结果[%s - %s] 查询条数%s", startDateTimeStr, endDateTimeStr, resultSize));
            synchronized (lock) {
                //推送完毕 最后使用endDateTime 设置为起始时间
                startDateTime = endDateTime;
            }
        }
    }

    @Override
    public void cancel() {
        //设置停止run方法循环
        isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //快照最新状态
        startDateTimeState.clear();
        startDateTimeState.add(startDateTime);
        System.out.println("快照结果: " + startDateTime);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
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
