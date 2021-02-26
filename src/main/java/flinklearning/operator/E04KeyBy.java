package flinklearning.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分组函数
 */
public class E04KeyBy {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //添加数据来源
        DataStreamSource<String> source = env.fromElements("香蕉", "苹果", "香蕉", "香蕉", "车厘子", "苹果");

        //将水果按照相同类型分组
        KeyedStream<String, String> keyedStream = source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String key) throws Exception {
                return key;
            }
        });

        //也可以按照元素位置  或者元素名称   数据是对象写法比较简单
//        source.keyBy(0);
//        source.keyBy("元素名称");

        //每次统计当前水果数量
        SingleOutputStreamOperator<String> operator = keyedStream.map(new MapFunction<String, String>() {
            private Integer count = 0;

            @Override
            public String map(String s) throws Exception {
                count++;
                return "当前：" + s + "数量：" + count;
            }
        });

        operator.print();

        env.execute("E04KeyByJob");
    }
}
