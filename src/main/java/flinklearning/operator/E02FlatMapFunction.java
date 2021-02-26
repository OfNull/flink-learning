package flinklearning.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 1 -> 0-n
 */
public class E02FlatMapFunction {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //添加数据来源
        DataStreamSource<String> source = env.fromElements("你好 我叫小黑 你叫什么呢", "I Name Is Bob");

        /**
         * 可以看到特性  一个元素进去 0到N个元素出来   可以对元素做任意转换处理
         */
        SingleOutputStreamOperator<String> operator = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String text, Collector<String> collector) throws Exception {
                String[] split = text.split(" ");
                for (String msg : split) {
                    collector.collect(msg);
                }
            }
        });

        operator.print();
        env.execute("ItMapJob");
    }
}
