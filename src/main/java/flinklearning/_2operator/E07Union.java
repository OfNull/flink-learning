package flinklearning._2operator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class E07Union {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 流a
         */
        DataStreamSource<String> aSource = env.fromElements("a", "b", "C");
        /**
         * 流b
         */
        DataStreamSource<String> bSource = env.fromElements("e", "f", "g");

        /**
         * a流连接b  成并集
         * 要求流的类型需要一致  比如这里都是 String类型
         */
        DataStream<String> union = aSource.union(bSource);

        union.print();

        env.execute("unionJob");

    }
}
