package flinklearning._1source;

import flinklearning._1source.model.OrderInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 带上下文 不支持并发
 * 而且增加了两个资源打开关闭方法 可以再run之前调用
 */
public class E03CustomizeRichSource extends RichSourceFunction<OrderInfo> {
    private transient Connection connection;
    private transient PreparedStatement ps;
    //运行标志位
    private volatile boolean isRunning = true;

    /**
     * run 之前调用
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //冲配置中读取文件
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String url = parameterTool.get("url");
        String username = parameterTool.get("username");
        String password = parameterTool.get("password");

        //初始化
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(url, username, password);
        ps = connection.prepareStatement("select * from order_info");

    }

    /**
     * 在线程中断前调用
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        //关闭资源
        isRunning = false;
        ps.close();
        connection.close();
    }

    @Override
    public void run(SourceContext<OrderInfo> ctx) throws Exception {
        try (ResultSet resultSet = ps.executeQuery()) {
            while (resultSet.next()) {
                //todo ...
                //ctx.
            }
        }
    }

    @Override
    public void cancel() {

    }
}
