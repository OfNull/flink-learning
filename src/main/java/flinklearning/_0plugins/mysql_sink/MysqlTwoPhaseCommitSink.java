package flinklearning._0plugins.mysql_sink;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlTwoPhaseCommitSink<IN> extends TwoPhaseCommitSinkFunction<IN, Connection, Void> {
    private final static Logger LOG = LoggerFactory.getLogger(MysqlTwoPhaseCommitSink.class);

    public MysqlTwoPhaseCommitSink(TypeSerializer<Connection> transactionSerializer, TypeSerializer<Void> contextSerializer) {
        super(transactionSerializer, contextSerializer);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    @Override
    protected void invoke(Connection transaction, IN value, Context context) throws Exception {
        PreparedStatement preparedStatement = transaction.prepareStatement("insert into span_item(span_type, app_name,item,compent_id,`date`) VALUES (?,?,?,?,?)");
        preparedStatement.setString(1, "Test");
        preparedStatement.setString(2, "zk");
        preparedStatement.setString(3, "/user");
        preparedStatement.setInt(4, 100);
        preparedStatement.setString(5, "20210505");
        preparedStatement.executeBatch();
    }

    @Override
    protected Connection beginTransaction() throws Exception {
        //创建连接资源trace.monitor.db.url=
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://10.130.36.244:3306/monitor-uat-trace?useUnicode=true&characterEncoding=UTF-8&useSSL=false&autoReconnect=true&&serverTimezone=Asia/Shanghai",
                "root", "tech789");
        boolean isSupportsTransaction = connection.getMetaData().supportsTransactions();
        if (!isSupportsTransaction) {
            throw new RuntimeException("当前连接不支持事务!");
        }
        connection.setAutoCommit(false);
        return connection;
    }

    @Override
    protected void preCommit(Connection transaction) throws Exception {
        //todo

    }

    @Override
    protected void commit(Connection transaction) {
        try {
            transaction.commit();
        } catch (SQLException e) {
            LOG.debug("transaction.commit() Happen SQLException ", e);
            throw new RuntimeException("事务提交异常", e);
        }
    }

    @Override
    protected void abort(Connection transaction) {
        try {
            transaction.rollback();
        } catch (SQLException e) {
            LOG.debug("transaction.rollback() Happen SQLException ", e);
            throw new RuntimeException("事务回滚异常", e);
        }
    }


}
