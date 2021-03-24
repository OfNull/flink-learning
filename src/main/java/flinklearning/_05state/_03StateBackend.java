package flinklearning._05state;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @Description TODO
 * @Date 2021/3/16 1:50 下午
 * @Created by zhoukun
 */
public class _03StateBackend {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);

        //基于内存状态 全量快照
//        MemoryStateBackend memoryStateBackend = new MemoryStateBackend();
//
//        //文件系统 全量快照
//        FsStateBackend fileBackend = new FsStateBackend("file:///usr/flink-1.12.1/tmp");
//        FsStateBackend hdfBackend = new FsStateBackend("hdfs:///checkpoints");
        // rocksDb 需要引入依赖  第二个参数是否开启增量检查点
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///Users/zhoukun/tmp/checkpoint", true);
        env.setStateBackend(rocksDBStateBackend);
        //过期
        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(60))
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .updateTtlOnCreateAndWrite()
                .cleanupInRocksdbCompactFilter(1000)
                .build();
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapCount =
                source.keyBy(v -> v)
                        .map(new _00CountRichMap(stateTtlConfig));

        mapCount.print();
        env.execute("countJob");
    }
}
