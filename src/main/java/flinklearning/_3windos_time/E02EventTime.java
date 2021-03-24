package flinklearning._3windos_time;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * @Description TODO
 * @Date 2021/3/16 3:49 下午
 * @Created by zhoukun
 */
public class E02EventTime {
    public static void main(String[] args) throws Exception {
        //1.12.0 默认时间特征是 EventTime
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 8888);

        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = source.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String input, Collector<Tuple2<String, Long>> out) throws Exception {
                if (input.contains(",")) {
                    String[] split = input.split(",");
                    System.out.println(String.format("当前数据Key: %s - 时间：%s", split[0], split[1]));
                    out.collect(Tuple2.of(split[0], Long.valueOf(split[1])));
                }
            }
        });
        //设置Watermarks策略
        SingleOutputStreamOperator<Tuple2<String, Long>> watermarksAssign = flatMap.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner((e, timestamp) -> {
                            System.out.println("当前时间：" + e.f1);
                            return e.f1;
                        }));
        watermarksAssign.flatMap(new RichFlatMapFunction<Tuple2<String, Long>, Object>() {
            @Override
            public void flatMap(Tuple2<String, Long> value, Collector<Object> out) throws Exception {

            }
        });

        SingleOutputStreamOperator<Tuple4<String, Long, Long, Integer>> processWindow = watermarksAssign.keyBy(e -> e.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(60))) //时区默认0区开始 中国必须设置  Time.hours(-8)
                .trigger(EventTimeTrigger.create())
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple4<String, Long, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple4<String, Long, Long, Integer>> out) throws Exception {
                        long startTime = context.window().getStart();
                        long endTime = context.window().getStart();
                        int count = 0;
                        for (Iterator<Tuple2<String, Long>> iterator = elements.iterator(); iterator.hasNext(); ) {
                            Tuple2<String, Long> next = iterator.next();
                            count += 1;
                        }
                        out.collect(Tuple4.of(key, startTime, endTime, count));
                    }
                });

        processWindow.print();

        env.execute("执行");
    }
}
