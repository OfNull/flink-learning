package flinklearning._3windos_time;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class E00Time {
    public static void main(String[] args) {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //添加数据源
        DataStreamSource<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("createTime", 20210211), Tuple2.of("createTime", 20210215), Tuple2.of("createTime", 20210218));


        source.keyBy(v->"").timeWindow(Time.seconds(10)).process(new ProcessWindowFunction<Tuple2<String, Integer>, Object, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Object> out) throws Exception {

            }
        });

    }
}
