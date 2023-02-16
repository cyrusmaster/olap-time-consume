package com.hzsun.flink.bigscreen.trigger;

import com.hzsun.flink.bigscreen.kafka.DebeziumStruct;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;


/**
  * REMARK   固定时间间隔触发    1000L = 1s
  * @className   FixedDelayTrigger
  * @date  2023/2/16 15:17
  * @author  cyf  
  */ 
public class FixedDelayTrigger extends Trigger<Integer, TimeWindow> {

	 @Override
	 public TriggerResult onElement(Integer debeziumStruct, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
		 ValueState<Boolean> partitionedState = triggerContext.getPartitionedState(new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN));
		 if (partitionedState.value() == null) {
			 for (long i = timeWindow.getStart(); i<timeWindow.getEnd(); i = i +1000L){
			 	triggerContext.registerEventTimeTimer(i);
			 }
			 partitionedState.update(true);
		 }



		 return TriggerResult.CONTINUE;
	 }

	 @Override
	 public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
		 return TriggerResult.CONTINUE;
	 }

	 @Override
	 public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

		 return TriggerResult.FIRE;
	 }

	 @Override
	 public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
	 	ValueState<Boolean> partitionedState = triggerContext.getPartitionedState(new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN));
	 	partitionedState.clear();
	 }
 }
