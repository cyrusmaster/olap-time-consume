package com.hzsun.flink.bigscreen.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @Description   流处理模式
 * @ClassName   OneByOneTrigger
 * @Date  2021/8/18 20:12
 * @Author   chenyongfeng
 * @ 遇事不决量子力学
 */
public class OneByOneTrigger extends Trigger<Object, TimeWindow> {


    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onProcessingTime(long element, TimeWindow timestamp, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

    }


}
