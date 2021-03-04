package flinklearning._1source;

import flinklearning._1source.model.TraceSegmentRecordInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * 实现自定实现 Mysql 数据读取器
 * 支持 定点读取和切好一次读取
 */
public class E05ReadMysqlRichSource01 extends RichSourceFunction<TraceSegmentRecordInfo> implements CheckpointedFunction {
    public E05ReadMysqlRichSource01() {
        super();
        System.err.println("第一执行 1构造方法");

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.err.println("第三执行 open---------------");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        System.err.println("第五执行 close---------------");
        super.close();
    }

    @Override
    public void run(SourceContext<TraceSegmentRecordInfo> ctx) throws Exception {
        System.err.println("第三四 run---------------");
    }

    @Override
    public void cancel() {
        System.err.println("取消时候执行 cancel---------------");
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.err.println("快照时候执行 snapshotState---------------");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        System.err.println("第二执行 initializeState---------------");
    }
}
