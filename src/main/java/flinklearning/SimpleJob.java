package flinklearning;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 简单Job
 */
public class SimpleJob {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //添加数据来源
        DataStreamSource<Integer> source = env.fromElements(1, 3, 5, 7, 9);

        //算子转换  内容+1
        SingleOutputStreamOperator<Integer> mapOperator = source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer + 1;
            }
        });

        //控制台打印输出
        mapOperator.print();

        //执行程序
        env.execute();
    }
}
