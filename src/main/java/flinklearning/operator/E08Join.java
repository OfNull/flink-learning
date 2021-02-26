package flinklearning.operator;

import flinklearning.operator.entity.Item;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;

/**
 * todo
 */
public class E08Join {

    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //商品
        DataStreamSource<Item> itemSource = env.fromElements(new Item("手机", 100), new Item("电脑", 50));
        //价格
        DataStreamSource<Tuple2<String, Double>> priceSource = env.fromElements(new Tuple2("手机", 1000.50), new Tuple2("电脑", 5000.10));

        JoinedStreams.WithWindow<Item, Tuple2<String, Double>, String, Window> window = itemSource.join(priceSource).where(new KeySelector<Item, String>() {
            @Override
            public String getKey(Item item) throws Exception {
                return item.getName();
            }
        }).equalTo(new KeySelector<Tuple2<String, Double>, String>() {
            @Override
            public String getKey(Tuple2<String, Double> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).window(new WindowAssigner<CoGroupedStreams.TaggedUnion<Item, Tuple2<String, Double>>, Window>() {
            @Override
            public Collection<Window> assignWindows(CoGroupedStreams.TaggedUnion<Item, Tuple2<String, Double>> itemTuple2TaggedUnion, long l, WindowAssignerContext windowAssignerContext) {
                System.out.println("assignWindows----------");
                return null;
            }

            @Override
            public Trigger<CoGroupedStreams.TaggedUnion<Item, Tuple2<String, Double>>, Window> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
                System.out.println("getDefaultTrigger----------");
                return null;
            }

            @Override
            public TypeSerializer<Window> getWindowSerializer(ExecutionConfig executionConfig) {
                System.out.println("getWindowSerializer----------");
                return null;
            }

            @Override
            public boolean isEventTime() {
                System.out.println("isEventTime----------");
                return false;
            }
        });

        window.apply(new JoinFunction<Item, Tuple2<String, Double>, Object>() {
            @Override
            public Object join(Item item, Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return null;
            }
        });
        env.execute("unionJob");
    }
}
