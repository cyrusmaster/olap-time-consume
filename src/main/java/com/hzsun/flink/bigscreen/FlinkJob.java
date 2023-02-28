package com.hzsun.flink.bigscreen;

import com.hzsun.flink.bigscreen.filter.Filter;
import com.hzsun.flink.bigscreen.kafka.DebeziumStruct;
import com.hzsun.flink.bigscreen.kafka.KafkaInfo;
import com.hzsun.flink.bigscreen.model.TodayConsumeVO;
import com.hzsun.flink.bigscreen.transformation.CountDistinctWithBitmap;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import org.apache.flink.util.Collector;



/**
 * @Description  时段消费
 * @ClassName   WaterJob
 * @Date  2021/8/26 10:11
 * @Author   chenyongfeng
 * @ 遇事不决量子力学
 */
public class FlinkJob {
    public static TodayConsumeVO  result;
    public static void main(String[] args) throws Exception{
        result = new TodayConsumeVO();
        // 1定义环境
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env .setParallelism(1);
        // 10版本中 需要指定 默认process
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2 source   - event stream   清洗过滤  旧版本水印API
        SingleOutputStreamOperator<DebeziumStruct> eventStream = env
        .addSource(KafkaInfo.getSource())
            .filter(Filter::debeFilter)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<DebeziumStruct>(Time.milliseconds(3000L)) {
                    @Override
                    public long extractTimestamp(DebeziumStruct debeziumStruct) {
                        long  l = (long) debeziumStruct.getAfter().get("DealTime");
                        // mark 测试et   观察乱序,是否适合做et
                        //System.out.println(TimeUtil.timeStampToTime(l));
                        return l ;
                    }
                });


        // supermarket stream  AccNum去重计算人数
        SingleOutputStreamOperator<Tuple2<String,Integer>> supermarketStream = eventStream.filter(t -> "1003".equals(t.getAfter().get("DealerNum")) || "1009".equals(t.getAfter().get("DealerNum")))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                //.trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                //.evictor(TimeEvictor.of(Time.seconds(0),true))
                .process(new CountDistinctWithBitmap())
                .map(t ->  Tuple2.of("supermarket", (Integer) t))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                ;

        // canteenStream    复制流
        SingleOutputStreamOperator<DebeziumStruct> canteenStream = eventStream.filter(t -> !"1003".equals(t.getAfter().get("DealerNum")) && !"1009".equals(t.getAfter().get("DealerNum")));

        // canteenStream-breakfastStream  6 、9   14-17
        SingleOutputStreamOperator<Tuple2<String,Integer>> breakfastStream = canteenStream.windowAll(SlidingEventTimeWindows.of(Time.hours(3), Time.days(1), Time.hours(-2)))
                //.trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                //.evictor(TimeEvictor.of(Time.seconds(0),true))
                .process(new CountDistinctWithBitmap())
                .map(t ->  Tuple2.of("breakfastStream",(Integer)t))
                .returns(Types.TUPLE(Types.STRING,Types.INT));
        // canteenStream-lunchStream 11、13   19-21
        SingleOutputStreamOperator<Tuple2<String,Integer>> lunchStream = canteenStream.windowAll(SlidingEventTimeWindows.of(Time.hours(2), Time.days(1), Time.hours(3)))
                //.trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                //.evictor(TimeEvictor.of(Time.seconds(0),true))
                .process(new CountDistinctWithBitmap())
                .map(t -> Tuple2.of("lunchStream",(Integer)t))
                .returns(Types.TUPLE(Types.STRING,Types.INT));
        // canteenStream-dinnerStream 16、19    0-3
        SingleOutputStreamOperator<Tuple2<String,Integer>> dinnerStream = canteenStream.windowAll(SlidingEventTimeWindows.of(Time.hours(3),Time.days(1), Time.hours(8)))
                //.trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                //.evictor(TimeEvictor.of(Time.seconds(0),true))
                .process(new CountDistinctWithBitmap())
                .map(t -> Tuple2.of("dinnerStream",(Integer)t))
                .returns(Types.TUPLE(Types.STRING,Types.INT));

        // 合流
        supermarketStream.union(breakfastStream,lunchStream,dinnerStream)
                .process(new ProcessFunction<Tuple2<String, Integer>, TodayConsumeVO>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, ProcessFunction<Tuple2<String, Integer>, TodayConsumeVO>.Context context, Collector<TodayConsumeVO> collector) throws Exception {
                        switch (stringIntegerTuple2.f0) {
                            case "supermarket":
                                result.setSupermarketNum(stringIntegerTuple2.f1);
                                break;
                            case "breakfastStream":
                                if (stringIntegerTuple2.f1 == 1){
                                    result.setBreakfastNum(1);
                                    result.setLunchNum(0);
                                    result.setDinnerNum(0);
                                    result.setSupermarketNum(0);
                                }else {
                                    result.setBreakfastNum(stringIntegerTuple2.f1);
                                }
                                break;
                            case "lunchStream":
                                result.setLunchNum(stringIntegerTuple2.f1);
                                break;
                            case "dinnerStream":
                                result.setDinnerNum(stringIntegerTuple2.f1);
                                break;
                        }
                        collector.collect(result);
                    }
                })
                .map(TodayConsumeVO::toString).addSink(KafkaInfo.getProducer())
                //.print("result")
                ;



        env.execute(FlinkJob.class.getSimpleName());



    }

}
