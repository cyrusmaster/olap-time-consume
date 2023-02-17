package com.hzsun.flink.bigscreen.utils.wintest;

import com.hzsun.flink.bigscreen.utils.TimestampsUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


/**
  * REMARK   1.10 水印窗口测试
  *	测试1: trigger 触发计算方式
  *	测试2: 事件时间 +8h 影响
  * 最周测试: 统计的事件是否在当天窗口聚合,以及第二天重新计算(对于此场景 水印只影响零点前后统计 )
  * @className   WinTest
  * @date  2023/2/17 9:46
  * @author  cyf
  */
public class WinTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);





		// 1 source
		//1,1676602800000,3   2,1676603800000,4  1,1676604800000,5
		DataStreamSource<String> stringDataStreamSource = env.socketTextStream("172.16.67.40", 7777);
		SingleOutputStreamOperator<Event> socket = stringDataStreamSource.map(new MapFunction<String, Event>() {
			@Override
			public Event map(String s) throws Exception {
				String[] split = s.split(",");
				return new Event(Integer.parseInt(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]));
			}
		});


		DataStreamSource<Event> eventDataStreamSource = env.fromElements(
				new Event(1, 1676602800000L,3),
				new Event(2, 1676603400000L,12),
				new Event(1, 1676603700000L,7),
				new Event(3, 1676604000000L,5)
		);


		KeyedStream<Event, Integer> keyedStream =
		eventDataStreamSource.keyBy(t -> t.getId());

		//  有序事件流  定义 event time watermark
		SingleOutputStreamOperator<Event> watermarks = socket
		.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Event>() {
			@Override
			public long extractAscendingTimestamp(Event event) {
				System.out.println("数据事件时间: "+TimestampsUtils.timeStampToTime(event.getTime()));
				return event.getTime();
			}
		});


		SingleOutputStreamOperator<Event> fee = watermarks
		.keyBy(Event::getId)
				.window(TumblingEventTimeWindows.of(Time.minutes(5)))
				//.trigger(new Trigger<Event, TimeWindow>() {
				//	@Override
				//	public TriggerResult onElement(Event event, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
				//
				//		//System.out.println("onElementtrigger 上一事件水印: "+TimestampsUtils.timeStampToTime(triggerContext.getCurrentWatermark()));
				//		System.out.println("onElementtrigger 此事件水印: "+TimestampsUtils.timeStampToTime(event.getTime()-1));
				//        System.out.println("onElementttrigger 此事件窗口:"+TimestampsUtils.timeStampToTime(timeWindow.getStart()) + "---" + TimestampsUtils.timeStampToTime(timeWindow.getEnd()));
				//		//System.out.println("onElement");
				//		//return TriggerResult.CONTINUE;
				//		return TriggerResult.FIRE;
				//
				//	}
				//
				//	@Override
				//	public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
				//		System.out.println("onProcessingTime");
				//		return null;
				//	}
				//
				//	@Override
				//	public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
				//		//System.out.println("onEventTime");
				//		//System.out.println("EventTime中 wm: "+triggerContext.getCurrentWatermark()+" prss:"+triggerContext.getCurrentProcessingTime());
				//
				//		//return TriggerResult.FIRE;
				//		return TriggerResult.CONTINUE;
				//	}
				//
				//	@Override
				//	public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
				//		System.out.println("clear");
				//	}
				//})
				//		.reduce(new ReduceFunction<Event>() {
				//	@Override
				//	public Event reduce(Event event, Event t1) throws Exception {
				//		System.out.println("触发了");
				//		return new Event(9,3213,event.getFee()+t1.getFee());
				//	}
				//})
				.sum("fee")
				;

				fee.print();


		//eventIntegerKeyedStream.reduce(new ReduceFunction<Event>() {
		//	@Override
		//	public Event reduce(Event event, Event t1) throws Exception {
		//		return new Event(9,3213,event.getFee()+t1.getFee());
		//	}
		//}).print();



		env.execute();










        //SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stringDataStreamSource.flatMap(new FlatMapFunction<String, String[]>() {
        //            @Override
        //            public void flatMap(String s, Collector<String[]> collector) throws Exception {
        //                String[] s1 = s.split(" ");
        //                collector.collect(s1);
        //            }
        //        }).map(new MapFunction<String[], Tuple2<String, Integer>>() {
        //            @Override
        //            public Tuple2<String, Integer> map(String[] strings) throws Exception {
        //
        //                Tuple2<String, Integer> stringIntegerTuple2 = null;
        //                for (String d : strings) {
        //                    stringIntegerTuple2 = new Tuple2<>(d, 1);
        //                }
        //                return  stringIntegerTuple2;
        //            }
        //        }).keyBy(0)
        //        .sum(1);








	}


}
