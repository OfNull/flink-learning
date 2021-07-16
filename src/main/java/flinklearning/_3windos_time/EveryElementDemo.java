package flinklearning._3windos_time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class EveryElementDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Long>> source = env.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                //手动输入 name,1000
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
        //水印抽取
        SingleOutputStreamOperator<Tuple2<String, Long>> streamOperator = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner((v, t) -> v.f1));

        SingleOutputStreamOperator<Integer> process = streamOperator.keyBy(v -> "")
                .window(TumblingEventTimeWindows.of(Time.of(60, TimeUnit.SECONDS)))
                .trigger(EveryElementEventTimeTrigger.create())
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Integer, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Integer> out) throws Exception {
                        Integer counter = 0;
                        for (Iterator<Tuple2<String, Long>> iterator = elements.iterator(); iterator.hasNext(); ) {
                            Tuple2<String, Long> next = iterator.next();
                            counter++;
                        }
                        out.collect(counter);
                        System.out.println("STR 触发提醒：" + context.window().getStart() + " - " + context.window().getEnd() + " 总数：" + counter);

                    }
                });

        process.print();
        env.execute("----Job----");
    }
}
