package flinklearning._1source;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Random;

public class E01SourceFunction {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.addSource(new SourceFunction<String>() {
            private boolean isRunning = true;


            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    long timeMillis = System.currentTimeMillis();
                    ctx.collectWithTimestamp(random.nextInt(10) + ".. . ..", timeMillis);
                    ctx.emitWatermark(new Watermark(timeMillis - 1));
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        DataStreamSource<String> dataStreamSource = env.addSource(new RichSourceFunction<String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                //用来打开资源
                System.out.println("open ------------");
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                //用来关闭
                System.out.println("close ---------");
                super.close();
            }

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int i = 0;
                while (true) {
                    System.out.println("1111111");
                    Thread.sleep(1000L);
                    if (i++ == 2) {
                        int a = 5 / 0;
                    }
                }
            }

            @Override
            public void cancel() {
                System.out.println("cancel ---------");
            }
        });

        env.addSource(new RichParallelSourceFunction<String>() {
            private boolean isRunning = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                Random random = new Random();

                while (isRunning) {
                    long timeMillis = System.currentTimeMillis();
                    ctx.collectWithTimestamp(random.nextInt(10) + ".. . .."+Thread.currentThread().getName(), timeMillis);
                    ctx.emitWatermark(new Watermark(timeMillis - 1));
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2).print();


        JobExecutionResult execute = env.execute("------");


    }
}
