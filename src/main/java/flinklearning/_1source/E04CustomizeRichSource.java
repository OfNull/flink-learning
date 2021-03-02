package flinklearning._1source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * 带上下文 支持并行
 */
public class E04CustomizeRichSource extends RichParallelSourceFunction<String> {
    private int scope = 0;
    //运行标志位
    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        int i = new Random().nextInt();
        this.scope = i;
        System.out.println(Thread.currentThread().getName() + "执行 open - i =" + scope);

        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        System.out.println(Thread.currentThread().getName() + "执行 close");
        super.close();
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(Thread.currentThread() + " " + scope);
            Thread.sleep(2000L);
        }
    }

    @Override
    public void cancel() {

    }
}
