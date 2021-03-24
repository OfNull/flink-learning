package flinklearning._05state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * @Description TODO
 * @Date 2021/3/16 3:17 下午
 * @Created by zhoukun
 */
public class _00CountRichMap extends RichMapFunction<String, Tuple2<String, Integer>> {
    private StateTtlConfig stateTtlConfig;
    private MapState<String, Integer> mapState;


    public _00CountRichMap(StateTtlConfig stateTtlConfig) {
        this.stateTtlConfig = stateTtlConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<String, Integer>("map", Types.STRING, Types.INT);
        mapStateDescriptor.enableTimeToLive(stateTtlConfig);
        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public Tuple2<String, Integer> map(String value) throws Exception {
        Integer count = mapState.get(value);
        if (count == null) {
            count = 1;
        } else {
            count += 1;
        }
        mapState.put(value, count);
        //只能清除当前key
        //mapState.clear();
        return Tuple2.of(value, count);
    }
}
