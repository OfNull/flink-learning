package flinklearning._05state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @Description 状态类型
 * @Date 2021/3/9 7:14 下午
 * @Created by zhoukun
 */

public class _01StateType {
    Logger logger = LoggerFactory.getLogger(_01StateType.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Flink 两种状态  OperateState  KeyedByState
//        DataStreamSource<String> outDataStreamSource = env.fromElements("A", "A", "v", "e");

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置文件系统作为状态后端
        FsStateBackend fsStateBackend = new FsStateBackend("file:///usr/flink-1.12.1/tmp");
        env.setStateBackend(fsStateBackend);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("127.0.0.1", 7777);
        //OperateState
        SingleOutputStreamOperator<String> map = dataStreamSource.map(new _01OperateStateRichMap()).setParallelism(4);

        map.print();
        env.execute("Operate-State");

        //KeyedBy
//        outDataStreamSource.keyBy(v -> v).map(new RichMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String s) throws Exception {
//                return null;
//            }
//        });


//        ValueState<String> valueState;
//        ListState<String> listState;
//        ReducingState<Integer> reducingState;
//        AggregatingState<Integer, String> aggregatingState;
//        MapState<String, Long> mapState;

        //  @org.apache.flink.annotation.PublicEvolving
        //    <T> org.apache.flink.api.common.state.ValueState<T> getState(org.apache.flink.api.common.state.ValueStateDescriptor<T> valueStateDescriptor);
        //
        //    @org.apache.flink.annotation.PublicEvolving
        //    <T> org.apache.flink.api.common.state.ListState<T> getListState(org.apache.flink.api.common.state.ListStateDescriptor<T> listStateDescriptor);
        //
        //    @org.apache.flink.annotation.PublicEvolving
        //    <T> org.apache.flink.api.common.state.ReducingState<T> getReducingState(org.apache.flink.api.common.state.ReducingStateDescriptor<T> reducingStateDescriptor);
        //
        //    @org.apache.flink.annotation.PublicEvolving
        //    <IN, ACC, OUT> org.apache.flink.api.common.state.AggregatingState<IN,OUT> getAggregatingState(org.apache.flink.api.common.state.AggregatingStateDescriptor<IN,ACC,OUT> aggregatingStateDescriptor);
        //
        //    @org.apache.flink.annotation.PublicEvolving
        //    <UK, UV> org.apache.flink.api.common.state.MapState<UK,UV> getMapState(org.apache.flink.api.common.state.MapStateDescriptor<UK,UV> mapStateDescriptor);
    }

    /**
     * KeyedBy
     */
    static class KeyByStateRichMapFunction extends RichMapFunction<String, Tuple2<String, Long>> {
        MapState<String, Long> mapState;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //定义状态声明 获取状态
            MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<String, Long>("mapState", Types.STRING, Types.LONG);
            mapState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public Tuple2<String, Long> map(String s) throws Exception {
            Long count = mapState.get(s);
            if (count != null) {
                count++;
            } else {
                count = 1L;
            }
            mapState.put(s, count);
            return Tuple2.of(s, count);
        }
    }

    static class _01OperateStateMap implements CheckpointedFunction, MapFunction<String, String> {
        //支持ListState
        private ListState<Integer> listState;
        List<Integer> list = new ArrayList<>();

        @Override
        public String map(String value) throws Exception {

            list.add(111);
            return null;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //开始快照
            listState.clear();
            for (Integer integer : list) {
                listState.add(integer);
            }

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //定义状态声明 获取状态
            ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<Integer>("listState", Types.INT);
            listState = context.getOperatorStateStore().getListState(listStateDescriptor);

            //是否从故障中恢复
            if (context.isRestored()) {
                listState.get().forEach(list::add);
            }
        }
    }

    /**
     * OperateState  实现 CheckpointedFunction
     * 并行度缩放时候， 3种状态各不相同
     * ListState p1 4 ->  p2  2 2
     * unionListState p2 2 3 -> p3   5 5
     * broadcastState  p2 1   1  -> p3 1 1 1
     */
    static class _01OperateStateRichMap extends RichMapFunction<String, String> implements CheckpointedFunction {
        Logger logger = LoggerFactory.getLogger(_01OperateStateRichMap.class);

        //只支持ListState
//        private ListState<Integer> listState;
        private ListState<Integer> unionListState;
        //支持
//        private BroadcastState<String, String> broadcastState;

        Map<String, String> map = new HashMap<>();
        List<Integer> list = new ArrayList<>();
        List<Integer> unionList = new ArrayList<>();

        @Override
        public String map(String s) throws Exception {
            String count = map.get(s);

            if (count != null) {
                Integer icount = Integer.valueOf(count);
                icount++;
                map.put(s, String.valueOf(icount));
                list.add(icount);
                unionList.add(icount);
            } else {
                map.put(s, "1");
                list.add(1);
                unionList.add(1);
            }
            return s;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            //快照数据
//            logger.info("listState 恢复开始 IndexOfThisSubtask:{} - NumberOfParallelSubtasks:{}", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
//            for (Integer integer : list) {
//                logger.info("listState 恢复了 {}", integer);
//                listState.add(integer);
//            }
//            logger.info("listState 快照结束 IndexOfThisSubtask:{} - NumberOfParallelSubtasks:{}", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
//
//
//            logger.info("MapState 快照开始 IndexOfThisSubtask:{} - NumberOfParallelSubtasks:{}", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
//            for (Map.Entry<String, String> entry : map.entrySet()) {
//                logger.info("listState 恢复了 {} - {}", entry.getKey(), entry.getValue());
//                broadcastState.put(entry.getKey(), entry.getValue());
//            }
//            logger.info("MapState 快照结束 IndexOfThisSubtask:{} - NumberOfParallelSubtasks:{}", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());

            logger.info("unionListState 恢复开始 IndexOfThisSubtask:{} - NumberOfParallelSubtasks:{}", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
            for (Integer integer : unionList) {
                logger.info("unionListState 恢复了 {}", integer);
                unionListState.add(integer);
            }
            logger.info("unionListState 快照结束 IndexOfThisSubtask:{} - NumberOfParallelSubtasks:{}", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());

        }

        //启动时候
        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            //定义状态声明 获取状态
//            ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<Integer>("listState", Types.INT);
//            listState = functionInitializationContext.getOperatorStateStore().getListState(listStateDescriptor);
//
//            MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>("mapState", Types.STRING, Types.STRING);
//            broadcastState = functionInitializationContext.getOperatorStateStore().getBroadcastState(mapStateDescriptor);

            ListStateDescriptor<Integer> unionListStateDescriptor = new ListStateDescriptor<Integer>("listState", Types.INT);
            unionListState = functionInitializationContext.getOperatorStateStore().getUnionListState(unionListStateDescriptor);

            //是否从故障中恢复
            if (functionInitializationContext.isRestored()) {
                //从状态
//                logger.info("MapState 恢复开始 IndexOfThisSubtask:{} - NumberOfParallelSubtasks:{}", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
//                for (Iterator<Map.Entry<String, String>> iterator = broadcastState.iterator(); iterator.hasNext(); ) {
//                    Map.Entry<String, String> next = iterator.next();
//                    logger.info("MapState 恢复了：{} -{}", next.getKey(), next.getValue());
//                    map.put(next.getKey(), next.getValue());
//                }
//                logger.info("MapState 恢复结束 IndexOfThisSubtask:{} - NumberOfParallelSubtasks:{}", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
//
//                logger.info("listState 恢复开始 IndexOfThisSubtask:{} - NumberOfParallelSubtasks:{}", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
//                listState.get().forEach(list::add);
//                for (Integer integer : listState.get()) {
//                    logger.info("listState 恢复了 {}", integer);
//                    list.add(integer);
//                }
//                logger.info("listState 恢复结束 IndexOfThisSubtask:{} - NumberOfParallelSubtasks:{}", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());

                logger.info("unionList 恢复开始 IndexOfThisSubtask:{} - NumberOfParallelSubtasks:{}", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());

//                unionListState.get().forEach(unionList::add);
                for (Integer integer : unionListState.get()) {
                    logger.info("unionList 恢复了 {}", integer);
                    unionList.add(integer);
                }
                logger.info("unionList 恢复结束 IndexOfThisSubtask:{} - NumberOfParallelSubtasks:{}", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());

            }

        }
    }

    /**
     * 竟然过期了
     * <p>
     * 只是 ListState 简化版
     */
    static class _02OperateStateRichMap extends RichMapFunction<String, String> implements ListCheckpointed {
        @Override
        public String map(String s) throws Exception {
            return null;
        }

        @Override
        public List snapshotState(long checkpointId, long timestamp) throws Exception {
            return null;
        }

        @Override
        public void restoreState(List state) throws Exception {

        }
    }


}
