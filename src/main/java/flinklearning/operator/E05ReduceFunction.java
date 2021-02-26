package flinklearning.operator;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 可以获取同一个key上一个发送出去的数据
 */
public class E05ReduceFunction {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //添加数据来源
        DataStreamSource<Integer> source = env.fromElements(2,2,2,2,2,2);

        //将数据分组 然后相同数据 内容相加 输出这个结果
        SingleOutputStreamOperator<Integer> reduce = source.keyBy(v -> v).reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer previousValue, Integer currentValue) throws Exception {
                System.out.println("上一个数据返回出去的数据：" + previousValue + "当前数据：" + currentValue);
                return previousValue + currentValue;
            }
        });

        reduce.print();

        env.execute("E05ReduceJob");
    }
}
