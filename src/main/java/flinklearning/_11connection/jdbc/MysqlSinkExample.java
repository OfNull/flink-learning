package flinklearning._11connection.jdbc;

public class MysqlSinkExample {

    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStreamSource<String> source = env.fromElements("a", "b", "c", "d");
//
//        //执行参数
//        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder().withBatchIntervalMs(500L).withBatchSize(10000).build();
//        //数据库连接参数
//        JdbcConnectionOptions connectionOptions = (new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()).withUrl("jdbcurl").withDriverName("db.driver").withUsername("db.username").withPassword("monitor.db.password").build();
//        //sql
//        final String sql = "INSERT INTO letter(name)VALUES(?)";
//        //输出
//        source.addSink(JdbcSink.sink(
//                sql,
//                (ps, v) -> {
//                    ps.setString(1, v);
//                },
//                executionOptions,
//                connectionOptions
//        ));
//
//        env.execute("sink mysql");

        ThreadLocal<String> threadLocal = new ThreadLocal<>();
        threadLocal.set("wngsicong");
        Thread thread = new Thread(() -> {
            threadLocal.set("zhoukun");
        });
        thread.start();
        String s = threadLocal.get();
        System.out.println("------------");

    }
}
