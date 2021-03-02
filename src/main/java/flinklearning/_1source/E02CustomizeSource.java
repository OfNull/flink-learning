package flinklearning._1source;

import flinklearning._1source.model.OrderInfo;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 不带上下文 不支持并发
 */
public class E02CustomizeSource implements SourceFunction<OrderInfo> {

    //运行标志位
    private volatile boolean isRunning = true;


    @Override
    public void run(SourceContext<OrderInfo> ctx) throws Exception {
        while (isRunning) {
            OrderInfo orderInfo = new OrderInfo();
            orderInfo.setOrderNo(System.currentTimeMillis() + "" + new Random().nextInt(10));
            orderInfo.setItemId(1000);
            orderInfo.setItemName("iphone 12pro max");
            orderInfo.setPrice(9800D);
            orderInfo.setQty(1);
            ctx.collect(orderInfo);
            Thread.sleep(new Random().nextInt(10) * 1000);

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
