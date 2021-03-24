package flinklearning._05state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description TODO
 * @Date 2021/3/15 8:16 下午
 * @Created by zhoukun
 */
public class _02KeyByState {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = dataStreamSource.keyBy(v -> v).map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            MapState<String, Integer> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<String, Integer>("map", Types.STRING, Types.INT);
               //过期状态清理 可以设置状态TTL  returnExpiredIfNotCleanedUp:设置可见行返回失效且没有清理的数据  UpdateType:设置更新策略 比如读取写入更新
                StateTtlConfig build = StateTtlConfig.newBuilder(Time.hours(1))
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
//                        .cleanupFullSnapshot() //全量快照清理  基于内存状态， RocksDBStateBackend 增量清理无效
//                        .cleanupInRocksdbCompactFilter()  RocksDBStateBackend压缩时清理
                        .cleanupIncrementally(10, true)   // RocksDBStateBackend 无效
                        .build();
                mapStateDescriptor.enableTimeToLive(build);
                mapState = getRuntimeContext().getMapState(mapStateDescriptor);
            }

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                Integer count = mapState.get(value);
                if (count == null) {
                    count = 0;
                } else {
                    count += 1;
                }
                mapState.put(value, count);
                //只能清楚当前key
//                mapState.clear();

                return Tuple2.of(value, count);
            }
        });

        map.print();

    }
}
