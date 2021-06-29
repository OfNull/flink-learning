package flinklearning._06cdc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class JdbcTest {
    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(10);
        ExecutorService service = Executors.newFixedThreadPool(10);


        for (int j = 0; j < 10; j++) {
            service.execute(()->{

                try {
                    Class.forName("com.mysql.cj.jdbc.Driver");
                    Connection connection = DriverManager.getConnection("jdbc:mysql://10.130.36.244:3306/monitor-uat?useUnicode=true&characterEncoding=UTF-8&useSSL=false&autoReconnect=true&&serverTimezone=Asia/Shanghai",
                            "root", "tech789");

                    PreparedStatement ps = connection.prepareStatement("insert into monitor_report_record_daily_test (md5_key, app_name, ip, statis_date, monitor_type, monitor_item, count,trace_id)  VALUES (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE  count = VALUES(count),trace_id =VALUES(trace_id) ");

                    String tarceId = "300";
                    for (int i = 0; i < 10; i++) {
                        ps.setString(1, "MDK"+i);
                        ps.setString(2, "A_Name");
                        ps.setString(3, "192.0.0.1");
                        ps.setLong(4, 20210618);
                        ps.setInt(5, 2);
                        ps.setString(6, "/db");
                        ps.setLong(7, i + 3);
                        ps.setString(8, tarceId);

//                        ps.setLong(9, i + 3);
//                        ps.setString(10, tarceId);
                        ps.addBatch();
                    }

                    ps.executeBatch();
                    ps.close();
                    connection.close();
                } catch (ClassNotFoundException | SQLException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    countDownLatch.countDown();
                }

            });
        }
        try {
            countDownLatch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        service.shutdown();
    }
}
