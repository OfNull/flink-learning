package flinklearning._2operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Map Function
 */
public class E01MapFunction {

    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //添加数据来源
        DataStreamSource<Integer> source = env.fromElements(1, 3, 5, 7, 9);

        /**
         * 可以看到特性  一个元素进去 一个元素  可以对元素做任意转换处理
         */
        SingleOutputStreamOperator<String> operator = source.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer integer) throws Exception {
                return "value = " + integer;
            }
        });

        operator.print();
        env.execute("ItMapJob");
    }
}
