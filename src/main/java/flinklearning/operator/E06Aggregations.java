package flinklearning.operator;

import flinklearning.operator.entity.Item;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 聚合函数
 */
public class E06Aggregations {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //添加数据来源
        DataStreamSource<Item> source = env.fromElements(new Item("手机", "淘宝", 10),
                new Item("电脑", "京东", 5),
                new Item("充电器", "京东", 100),
                new Item("手机", "京东", 50),
                new Item("手机", "拼多多", 20));

        /**
         * 返回同一分组下 最大的数量的商品
         * maxBy 会更新整条数据字段信息
         * max   只会去第一条数据信息
         */
        SingleOutputStreamOperator<Item> maxBy = source.keyBy(Item::getName).maxBy("qty");
        SingleOutputStreamOperator<Item> max = source.keyBy(Item::getName).max("qty");


        /**
         * 返回同一分组下 最小的数量的商品
         * maxBy 会更新整条数据字段信息
         * max   只会去第一条数据信息
         */
        SingleOutputStreamOperator<Item> min = source.keyBy(Item::getName).min("qty");
        SingleOutputStreamOperator<Item> minBy = source.keyBy(Item::getName).minBy("qty");

        /**
         * 统计 同一分组下 总qty量
         */
        SingleOutputStreamOperator<Item> sumQty = source.keyBy(Item::getName).sum("qty");

        maxBy.print();

        env.execute("A_");
    }

}
