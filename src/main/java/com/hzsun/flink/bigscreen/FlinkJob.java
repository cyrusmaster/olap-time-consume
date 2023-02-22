package com.hzsun.flink.bigscreen;

import com.hzsun.flink.bigscreen.filter.Filter;
import com.hzsun.flink.bigscreen.kafka.DebeziumStruct;
import com.hzsun.flink.bigscreen.kafka.KafkaInfo;
import com.hzsun.flink.bigscreen.model.TodayConsumeVO;
import com.hzsun.flink.bigscreen.transformation.CountDistinctWithBitmap;
import com.hzsun.flink.bigscreen.utils.TimeUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;


import java.time.Instant;
import java.util.Iterator;


/**
 * @Description  时段消费
 * @ClassName   WaterJob
 * @Date  2021/8/26 10:11
 * @Author   chenyongfeng
 * @ 遇事不决量子力学
 */
public class FlinkJob {
    // todo
    //private static final Logger LOG = LoggerFactory.getLogger(FlinkJob.class);


    public static void main(String[] args) throws Exception{
        TodayConsumeVO todayConsumeVO = new TodayConsumeVO();
        todayConsumeVO.setEventTime(Instant.now().toEpochMilli());
        int flag = 1;
        // 1定义环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                        //1
                        long  l = (long) debeziumStruct.getAfter().get("DealTime");

                        System.out.println(!TimeUtil.isSameDay(l,todayConsumeVO.getEventTime()));

                        if (!TimeUtil.isSameDay(l,todayConsumeVO.getEventTime())){
                            todayConsumeVO.setSupermarketNum(0);
                            todayConsumeVO.setBreakfastNum(0);
                            todayConsumeVO.setLunchNum(0);
                            todayConsumeVO.setDinnerNum(0);
                        }

                        todayConsumeVO.setEventTime(l);
                        //2
                        //long l2 = Long.parseLong(String.valueOf(debeziumStruct.getAfter().get("DealTime")));
                        // mark 测试et   观察乱序,是否适合做et
                        //System.out.println(TimeUtil.timeStampToTime(l));
                        return l ;
                    }
                });



        // supermarket stream  AccNum去重计算人数
        SingleOutputStreamOperator<Tuple2<String,Integer>> supermarketStream = eventStream.filter(t -> "1003".equals(t.getAfter().get("DealerNum")) || "1009".equals(t.getAfter().get("DealerNum")))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                .process(new CountDistinctWithBitmap())
                .map(t ->  Tuple2.of("supermarket", (Integer) t))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                ;


        // canteenStream    复制流
        SingleOutputStreamOperator<DebeziumStruct> canteenStream = eventStream.filter(t -> !"1003".equals(t.getAfter().get("DealerNum")) && !"1009".equals(t.getAfter().get("DealerNum")));
        //eventStream.print();
        //canteenStream.print();

        // canteenStream-breakfastStream  6 、9   14-17
        SingleOutputStreamOperator<Tuple2<String,Integer>> breakfastStream = canteenStream.windowAll(SlidingEventTimeWindows.of(Time.hours(3), Time.days(1), Time.hours(-2)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                .process(new CountDistinctWithBitmap())
                .map(t ->  Tuple2.of("breakfastStream",(Integer)t))
                .returns(Types.TUPLE(Types.STRING,Types.INT));
        // canteenStream-lunchStream 11、13   19-21
        SingleOutputStreamOperator<Tuple2<String,Integer>> lunchStream = canteenStream.windowAll(SlidingEventTimeWindows.of(Time.hours(2), Time.days(1), Time.hours(3)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                .process(new CountDistinctWithBitmap())
                .map(t -> Tuple2.of("lunchStream",(Integer)t))
                .returns(Types.TUPLE(Types.STRING,Types.INT));
        // canteenStream-dinnerStream 16、19    0-3
        SingleOutputStreamOperator<Tuple2<String,Integer>> dinnerStream = canteenStream.windowAll(SlidingEventTimeWindows.of(Time.hours(3),Time.days(1), Time.hours(8)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                .process(new CountDistinctWithBitmap())
                .map(t -> Tuple2.of("dinnerStream",(Integer)t))
                .returns(Types.TUPLE(Types.STRING,Types.INT));

        // 合流
        supermarketStream.union(breakfastStream)
                .union(lunchStream)
                        .union(dinnerStream)
                                .map(new MapFunction<Tuple2<String, Integer>, TodayConsumeVO>() {


                                    @Override
                                    public TodayConsumeVO map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {

                                        // 判断时间戳是否是同一天
                                        switch (stringIntegerTuple2.f0){
                                            case "supermarket":
                                                System.out.println("supermarket"+stringIntegerTuple2.f1);
                                                System.out.println("supermarket"+todayConsumeVO.getSupermarketNum());
                                                todayConsumeVO.setSupermarketNum(stringIntegerTuple2.f1);
                                            break;

                                            case "breakfastStream":
                                                System.out.println("breakfastStream"+stringIntegerTuple2.f1);
                                                System.out.println("breakfastStream"+todayConsumeVO.getBreakfastNum());
                                                todayConsumeVO.setBreakfastNum(stringIntegerTuple2.f1);
                                            break;

                                            case "lunchStream":
                                            System.out.println("lunchStream"+stringIntegerTuple2.f1);
                                            System.out.println("lunchStream"+todayConsumeVO.getLunchNum());
                                                todayConsumeVO.setLunchNum(stringIntegerTuple2.f1);
                                            break;

                                            case "dinnerStream":
                                            System.out.println("dinnerStream"+stringIntegerTuple2.f1);
                                                todayConsumeVO.setDinnerNum(stringIntegerTuple2.f1);
                                            break;
                                        }

                                        return todayConsumeVO;
                                    }
                                })
                .map(TodayConsumeVO::toString)
                .print()
                        //.addSink(KafkaInfo.getProducer())
                        ;




        env.execute(FlinkJob.class.getSimpleName());



//
////        supermarketNumStream.print();
//
////        合流   按照二元组类型汇总
//        ConsumeNumDTO consumeNumDTO = new ConsumeNumDTO();
//        DataStream <ConsumeNumDTO> TotalStream = breakfastStream
//                .union(lunchStream)
//                .union(dinnerStream)
//                .connect(supermarketNumStream)
//                .map(new CoMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, ConsumeNumDTO>() {
//
//                    @Override
//                    public ConsumeNumDTO map1(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//
//                        switch (stringIntegerTuple2.f0){
//                            case "morning":
//                                if (consumeNumDTO.getBreakfastNum() != stringIntegerTuple2.f1){
//                                    consumeNumDTO.setBreakfastNum(stringIntegerTuple2.f1);
//                                }
//                                break;
//                            case "noon":
//                                if(consumeNumDTO.getLunchNum() != stringIntegerTuple2.f1){
//                                    consumeNumDTO.setLunchNum(stringIntegerTuple2.f1);
//                                }
//                                break;
//                            case "night":
//                                if(consumeNumDTO.getDinnerNum() != stringIntegerTuple2.f1){
//                                    consumeNumDTO.setDinnerNum(stringIntegerTuple2.f1);
//                                }
//                                break;
//                            default:
//                                break;
//                        }
//                        return consumeNumDTO;
//                    }
//
//                    @Override
//                    public ConsumeNumDTO map2(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                        switch (stringIntegerTuple2.f0){
//                            case "market":
//                                if(consumeNumDTO.getSupermarketNum() != stringIntegerTuple2.f1){
//                                    consumeNumDTO.setSupermarketNum(stringIntegerTuple2.f1);
//                                }
//                                break;
//                            default:
//                                break;
//                        }
//                        return consumeNumDTO;
//                    }
//                })
//                .connect(timeStream)
//                .map(new CoMapFunction<ConsumeNumDTO, Tuple2<String, Long>, ConsumeNumDTO>() {
//                    @Override
//                    public ConsumeNumDTO map1(ConsumeNumDTO value) throws Exception {
//                        consumeNumDTO.setBreakfastNum(value.getBreakfastNum());
//                        consumeNumDTO.setLunchNum(value.getLunchNum());
//                        consumeNumDTO.setDinnerNum(value.getDinnerNum());
//                        consumeNumDTO.setSupermarketNum(value.getSupermarketNum());
//                        return consumeNumDTO;
//                    }
//
//                    @Override
//                    public ConsumeNumDTO map2(Tuple2<String, Long> value) throws Exception {
//                        consumeNumDTO.setTime(value.f1);
//                        return consumeNumDTO;
//                    }
//                });
//
////        TotalStream.print();
//
//
//        TotalStream
//                .map(new MapFunction<ConsumeNumDTO, String>() {
//                    @Override
//                    public  String map(ConsumeNumDTO value) throws Exception{
//                        return value.toString();
//                    }
//                })
//                 .print();
//               //.addSink(KafkaInfo.getProducer());
    }

}
