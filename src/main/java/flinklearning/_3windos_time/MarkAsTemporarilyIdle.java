package flinklearning._3windos_time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Scanner;

/**
 * 空闲水印
 */
public class MarkAsTemporarilyIdle {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Tuple2<String, Long>> source = env.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                Scanner scanner = new Scanner(System.in);
                while (true) {
                    if (scanner.hasNext()) {
                        String next = scanner.next();
                        String[] split = next.split(",");
                        System.out.println("输入了：" + split[0] + " : " + split[1]);
                        ctx.collect(Tuple2.of(split[0], Long.valueOf(split[1])));
                    }
                }
            }

            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> streamOperator = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner((t, w) -> t.f1).withIdleness(Duration.ofSeconds(10)));
        streamOperator.keyBy(e -> e.f1).window(TumblingEventTimeWindows.of(Time.seconds(5))).process(new ProcessWindowFunction<Tuple2<String, Long>, String, Long, TimeWindow>() {
            @Override
            public void process(Long aLong, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                System.out.println("STR 触发提醒：" + context.window().getStart() + " - " + context.window().getEnd());
                elements.iterator().forEachRemaining(o-> System.out.println(o));
                System.out.println("END 触发提醒：" + context.window().getStart() + " - " + context.window().getEnd());
            }
        });




        env.execute("sss");

    }
}
