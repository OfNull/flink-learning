package flinklearning._1source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class E01CustomizeSource implements SourceFunction<String> {
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        
    }

    @Override
    public void cancel() {

    }
}
