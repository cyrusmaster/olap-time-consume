package com.hzsun.flink.bigscreen.transformation;


import com.hzsun.flink.bigscreen.kafka.DebeziumStruct;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Iterator;

/**
  * REMARK   基于RoaringBitmap   https://www.jianshu.com/p/201b45f2a324
  * @className   CountDistinctWithBitmap
  * @date  2023/2/20 17:35
  * @author  cyf  
  */ 
public class CountDistinctWithBitmap extends ProcessAllWindowFunction<DebeziumStruct, Object, TimeWindow> {

  private transient ValueState<Roaring64NavigableMap> bitMapState;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			// 定义ValueStateDescriptor
			ValueStateDescriptor<Roaring64NavigableMap> bitmap = new ValueStateDescriptor<>("bitmap", TypeInformation.of(new TypeHint<Roaring64NavigableMap>() {
			}));
			StateTtlConfig build = StateTtlConfig
					// 过期时间设为6h
					.newBuilder(Time.hours(6))
					//.newBuilder(Time.days(1))
					// 在状态值被创建和被更新时重设TTL
					.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
					//已经过期的数据不能再被访问到；
					.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
					.build();
			bitmap.enableTimeToLive(build);
			// 初始化
			bitMapState = getRuntimeContext().getState(bitmap);
		}

		@Override
		public void process(ProcessAllWindowFunction<DebeziumStruct, Object, TimeWindow>.Context context, Iterable<DebeziumStruct> iterable, Collector<Object> collector) throws Exception {



			Roaring64NavigableMap value = bitMapState.value();
			if (value == null) {
				value = new Roaring64NavigableMap();
			}
			Iterator<DebeziumStruct> iterator = iterable.iterator();
			while (iterator.hasNext()) {
				//System.out.println(iterator.next());
				int accNum = Integer.parseInt(String.valueOf(iterator.next().getAfter().get("AccNum"))) ;
				value.add(accNum);
			}
			int intCardinality = value.getIntCardinality();
			collector.collect(intCardinality);

		}


}
