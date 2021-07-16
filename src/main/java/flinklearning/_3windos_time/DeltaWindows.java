package flinklearning._3windos_time;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class DeltaWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromElements(1, 3, 5, 9, 20, 100, 100, 200);

        source.keyBy(v -> "")
                .window(TumblingProcessingTimeWindows.of(Time.of(60, TimeUnit.SECONDS)))
                .trigger(PurgingTrigger.of(ProcessingTimeTrigger.create())) //触发器
                .process(new ProcessWindowFunction<Integer, Integer, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
                        //处理逻辑
                    }
                });


        SingleOutputStreamOperator<Integer> process = source.keyBy(v -> "")
                .window(TumblingProcessingTimeWindows.of(Time.of(60, TimeUnit.SECONDS)))
                .trigger(PurgingTrigger.of(DeltaTrigger.of(4, new DeltaFunction<Integer>() {
                    @Override
                    public double getDelta(Integer oldDataPoint, Integer newDataPoint) {
                        return newDataPoint - oldDataPoint;
                    }
                }, new IntSerializer())))
                .process(new ProcessWindowFunction<Integer, Integer, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {

                        Integer total = 0;
                        for (Iterator<Integer> iterator = elements.iterator(); iterator.hasNext(); ) {
                            Integer next = iterator.next();
                            total += next;
                        }
                        System.out.println("STR 触发提醒：" + context.window().getStart() + " - " + context.window().getEnd() + "总数：" + total);
                    }
                });


        process.print();
        env.execute("----");
    }
}
