package flinklearning.operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 过滤 1 -> 1- 0
 */
public class E03FilterFunction {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //添加数据来源
        DataStreamSource<Integer> source = env.fromElements(1, 3, 5, 7, 9);

        /**
         * 可以看到特性  一个元素进去 1 - 0个元素出来 过滤一些数据
         */
        SingleOutputStreamOperator<Integer> filter = source.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer == 3;
            }
        });

        filter.print();
        env.execute("ItMapJob");
    }
}
