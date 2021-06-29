package flinklearning._10hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.ZoneId;

public class HiveDemo {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\Users\\Admin\\Desktop\\hadoop\\hadoop-2.7.5");
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(environmentSettings);
       // SET time-zone=+08:00;
//        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
//        ZoneId localTimeZone = tableEnv.getConfig().getLocalTimeZone();
//        System.out.println(localTimeZone.getId());
        String name = "myhive";
        String defaultDatabase = "flink";
        String hiveConfDir = "C:/Users/Admin/Desktop/fsdownload"; // a local path

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");
        tableEnv.useDatabase(defaultDatabase);

        //hive方言
//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS person_hiv (\n" +
//                "  name STRING,\n" +
//                "  age INT,\n" +
//                "  ts TIMESTAMP\n" +
//                ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (\n" +
//                "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
//                "  'sink.partition-commit.trigger'='partition-time',\n" +
//                "  'sink.partition-commit.delay'='1 h',\n" +
//                "  'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
//                ")");


        //flink方言
//        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
//        tableEnv.executeSql("CREATE TABLE person_kaf(\n" +
//                "  name STRING,\n" +
//                "  age INT,\n" +
//                "  ts BIGINT,\n" +
//                "  ets AS TO_TIMESTAMP(FROM_UNIXTIME(`ts` / 1000,  'yyyy-MM-dd HH:mm:ss')),\n" +
//                "  WATERMARK FOR ets AS ets - INTERVAL '5' SECOND\n" +
//                ") WITH (\n" +
//                "    'connector' = 'kafka',\n" +
//                "    'topic' = 'hive-topic',\n" +
//                "    'properties.bootstrap.servers' = '10.130.11.208:9092',\n" +
//                "    'properties.group.id' = 'hiveGroup',\n" +
//                "    'format' = 'json',\n" +
//                "    'scan.startup.mode' = 'earliest-offset'\n" +
//                ")");


//        TableResult tableResult = tableEnv.executeSql(" SELECT name, age, ets, DATE_FORMAT(ets, 'yyyy-MM-dd'), DATE_FORMAT(ets, 'HH') FROM person_kaf");
        TableResult tableResult = tableEnv.executeSql(" INSERT INTO person_hiv SELECT name, age, ets, DATE_FORMAT(ets, 'yyyy-MM-dd'), DATE_FORMAT(ets, 'HH') FROM person_kaf");
//        tableResult.print();
//        TableResult tableResult = tableEnv.executeSql("SELECT * FROM person_hiv");
//        tableResult.print();


    }
}
