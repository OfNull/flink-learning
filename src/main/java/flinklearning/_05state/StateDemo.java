package flinklearning._05state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class StateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.fromElements("A", "B", "C", "D", "E", "F");

        SingleOutputStreamOperator<String> process = source.keyBy(v -> v).process(new KeyedProcessFunction<String, String, String>() {
            ValueState<String> vState;
            ValueState<Integer> v2State;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<String>("aa", Types.STRING);
                ValueStateDescriptor<Integer> descriptor2 = new ValueStateDescriptor<Integer>("bb", Types.INT, 0);
                vState = getRuntimeContext().getState(descriptor);
                v2State = getRuntimeContext().getState(descriptor2);
            }

            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                String value1 = vState.value();
                Integer value2 = v2State.value();
                System.out.println("----------");
            }
        }).setParallelism(2);
        process.print();

        env.execute("ABC");
    }
}
