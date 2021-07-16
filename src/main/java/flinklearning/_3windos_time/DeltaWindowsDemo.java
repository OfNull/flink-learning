package flinklearning._3windos_time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Scanner;

public class DeltaWindowsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source = env.addSource(new SourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                //手动输入元素 格式李白,1000
                Scanner scanner = new Scanner(System.in);
                while (true) {
                    if (scanner.hasNext()) {
                        String next = scanner.next();

                        System.out.println("输入了：" + next);
                        ctx.collect(Integer.valueOf(next));
                    }

                }
            }

            @Override
            public void cancel() {

            }
        });
        //抽取 水印
        SingleOutputStreamOperator<Integer> streamOperator = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Integer>forMonotonousTimestamps().withTimestampAssigner((v, t) -> v));

        streamOperator.windowAll(GlobalWindows.create())
                .trigger(DeltaTrigger.of(20000, new DeltaFunction<Integer>() {
                    @Override
                    public double getDelta(Integer oldDataPoint, Integer newDataPoint) {
                        return newDataPoint - oldDataPoint;
                    }
                }, new IntSerializer()))
                .process(new ProcessAllWindowFunction<Integer, Object, GlobalWindow>() {
                    @Override
                    public void process(Context context, Iterable<Integer> elements, Collector<Object> out) throws Exception {
                        Integer total = 0;
                        int count = 0;
                        for (Iterator<Integer> iterator = elements.iterator(); iterator.hasNext(); ) {
                            Integer next = iterator.next();
                            total += next;
                            count++;
                        }
                        System.out.println("STR 触发提醒：" + "总数：" + total + "  总条数：" + count);
                    }
                });
        //窗口统计
//        SingleOutputStreamOperator<Integer> process = streamOperator.keyBy(v -> "")
//                .window(TumblingEventTimeWindows.of(Time.of(60, TimeUnit.SECONDS)))
//                .trigger(DeltaTrigger.of(20000, new DeltaFunction<Integer>() {
//                    @Override
//                    public double getDelta(Integer oldDataPoint, Integer newDataPoint) {
//                        return newDataPoint - oldDataPoint;
//                    }
//                }, new IntSerializer()))
//                .process(new ProcessWindowFunction<Integer, Integer, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
//                        Integer total = 0;
//                        int count = 0;
//                        for (Iterator<Integer> iterator = elements.iterator(); iterator.hasNext(); ) {
//                            Integer next = iterator.next();
//                            total += next;
//                            count++;
//                        }
//                        System.out.println("STR 触发提醒：" + context.window().getStart() + " - " + context.window().getEnd() + "总数：" + total + "  总条数：" + count);
//                    }
//                });

//        process.print();

        env.execute("--------------");
    }
}
