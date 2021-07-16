package flinklearning._3windos_time;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class EveryElementEventTimeTrigger<W extends Window> extends Trigger<Object, W> {
    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        ctx.registerProcessingTimeTimer(window.maxTimestamp());
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        //如果维护了状态， 也可以在这里做状态清理！
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    public static EveryElementEventTimeTrigger create() {
        return new EveryElementEventTimeTrigger();
    }
}
