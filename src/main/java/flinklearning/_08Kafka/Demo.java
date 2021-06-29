package flinklearning._08Kafka;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Random;

public class Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.fromElements("a", "b", "c", "d");
        SingleOutputStreamOperator<String> process = source.rebalance().flatMap(new RichFlatMapFunction<String, String>() {

            //            private transient String str = new String(String.valueOf(new Random().nextInt(10)));
//            private transient String str = null;

            private transient String str = new String("Test");

            @Override
            public void open(Configuration parameters) throws Exception {

                super.open(parameters);
                try {
                    str = new String(String.valueOf(new Random().nextInt(10)));
                    System.out.println(String.format("%s - %s", getRuntimeContext().getIndexOfThisSubtask(), str));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void flatMap(String v, Collector<String> collector) throws Exception {
                System.out.println(String.format("%s - %s - %s - %s", Thread.currentThread().getName(), v, getRuntimeContext().getIndexOfThisSubtask(), str.toString()));
                collector.collect(v);
            }
        }).setParallelism(4);
        process.print();
        env.execute();

    }
}
