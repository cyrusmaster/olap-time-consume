package com.hzsun.flink.bigscreen.transformation;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

/**
  * REMARK   基于RoaringBitmap   https://www.jianshu.com/p/201b45f2a324
  * @className   CountDistinctWithBitmap
  * @date  2023/2/20 17:35
  * @author  cyf  
  */ 
public class CountDistinctWithBitmap extends ProcessFunction<Tuple,Tuple1<Integer>,Tuple1<Long>> {

	private ValueState<Tuple2<RoaringBitmap, Long>> state;

	@Override
  	public void open(Configuration parameters) throws Exception {
    	//状态初始化
    	state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", Types.TUPLE(Types.GENERIC(RoaringBitmap.class), Types.LONG)));

  	}



	@Override
	public void processElement(Tuple1<Integer> in, Context context, Collector<Tuple1<Long>> out) throws Exception {

	}


	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector out) throws Exception {
    }

}
