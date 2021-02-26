package flinklearning._3windos_time;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 系统时间
 */
public class E01ProcessTime {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置时间
        //1.12以前 现在过期了
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 3, 5, 7);
        integerDataStreamSource.map(new MapFunction<Integer, Object>() {
            @Override
            public Object map(Integer value) throws Exception {
                return null;
            }
        });

        env.execute("123");
    }
}
