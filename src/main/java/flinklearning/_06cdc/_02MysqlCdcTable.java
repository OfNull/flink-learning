package flinklearning._06cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class _02MysqlCdcTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 60 * 10);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000 * 60 * 10);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);
        EnvironmentSettings envSet = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, envSet);
        String sourceDDL = "" +
                "CREATE TABLE monitor_heart_record_binlog (\n" +
                "    id INT NOT NULL,\n" +
                "    service_name STRING,\n" +
                "    ip STRING,\n" +
                "    type STRING,\n" +
                "    last_heart_time BIGINT,\n" +
                "    create_time TIMESTAMP\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = '10.130.36.244',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'user',\n" +
                "    'password' = '123456',\n" +
                "    'database-name' = 'monitor-uat',\n" +
                "    'table-name' = 'monitor_heart_record',\n" +
                "    'server-time-zone' = 'Asia/Shanghai',\n" +
                "    'server-id' = '4008',\n" +
                "    'debezium.snapshot.locking.mode' = 'none'\n" +
                ")";

        String printDDL = "" +
                "CREATE TABLE monitor_heart_record_print (\n" +
                "    id INT,\n" +
                "    service_name STRING,\n" +
                "    ip STRING,\n" +
                "    type STRING,\n" +
                "    last_heart_time BIGINT,\n" +
                "    create_time TIMESTAMP,\n" +
                "    PRIMARY KEY (ip) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ")";

        String sinkKafkaDDL = "" +
                "CREATE TABLE topic_products(\n" +
                "    type STRING,\n" +
                "    total BIGINT,\n" +
                "    PRIMARY KEY (type) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'products_binlog',\n" +
                "    'properties.bootstrap.servers' = '10.130.11.208:9092,10.130.10.243:9092,10.130.11.207:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'format' = 'debezium-json'\n" +
//                "    'debezium-json.schema-include' = 'true'" + //是否包含 schema 信息 建议不要
                ")";

        String countDDL = "" +
                "CREATE TABLE monitor_heart_record_count_print (\n" +
                "    type STRING,\n" +
                "    total BIGINT,\n" +
                "    PRIMARY KEY (type) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ")";

        String insertDDL = "" +
                "INSERT INTO monitor_heart_record_print \n" +
                "    SELECT id, service_name, ip, type, last_heart_time, create_time \n" +
                "    FROM monitor_heart_record_binlog";


        String insertDDL2 = "" +
                "INSERT INTO monitor_heart_record_count_print \n" +
                "SELECT type, count(ip) as total  FROM monitor_heart_record_binlog group by type";

        String insertDDL3 = "" +
                "INSERT INTO topic_products \n" +
                "SELECT type, count(ip) as total  FROM monitor_heart_record_binlog group by type";
        tableEnv.executeSql(sourceDDL);
//        tableEnv.executeSql(printDDL);
        tableEnv.executeSql(countDDL);
//        tableEnv.executeSql(sinkKafkaDDL);
//        tableEnv.executeSql(insertDDL2);

        TableResult tableResult =   tableEnv.executeSql(insertDDL2);
//        TableResult tableResult = tableEnv.executeSql(insertDDL3);
        tableResult.print();
    }
}
