package com.hzsun.flink.bigscreen.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


 /**
  * REMARK     每来一个事件触发
  * @className   OneByOneTrigger
  * @date  2023/2/16 15:17
  * @author  cyf
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
