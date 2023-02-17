package com.hzsun.flink.bigscreen.utils;

import com.hzsun.flink.bigscreen.filter.Filter;
import com.hzsun.flink.bigscreen.kafka.DebeziumStruct;
import com.hzsun.flink.bigscreen.kafka.KafkaInfo;
import com.hzsun.flink.bigscreen.trigger.FixedDelayTrigger;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;

import java.util.HashSet;


/**
 * @Description  时段消费
 * @ClassName   WaterJob
 * @Date  2021/8/26 10:11
 * @Author   chenyongfeng
 * @ 遇事不决量子力学
 */
public class FlinkJobTest {
    // todo 测试日志
    //private static final Logger LOG = LoggerFactory.getLogger(FlinkJob.class);


    public static void main(String[] args) throws Exception{

        // 1定义环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env .setParallelism(1);
        // 10版本中 streaming programs need to set the time characteristic accordingly.
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2 source -> event stream
        SingleOutputStreamOperator<DebeziumStruct> mainStream = env
        .addSource(KafkaInfo.getSource())
                .filter(Filter::debeFilter)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<DebeziumStruct>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(DebeziumStruct debeziumStruct) {
                        System.out.println();

                        return (long)debeziumStruct.getAfter().get("DealTime") ;
                    }
                });
HashSet<Integer> integers = new HashSet<>();
        //分流 supermarketStream  滚动1d  做聚合  1s触发计算
        SingleOutputStreamOperator<Integer> dealerNum = mainStream
        .filter(t -> "1003".equals( t.getAfter().get("DealerNum")) || "1009".equals( t.getAfter().get("DealerNum")) )
                .map(t ->Integer.valueOf((String)t.getAfter().get("AccNum")))
                .windowAll(TumblingEventTimeWindows.of(Time.days(1),Time.hours(-8)))
                        //.trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
                        .trigger(new FixedDelayTrigger())
                                .reduce(new ReduceFunction<Integer>() {
                                    @Override
                                    public Integer reduce(Integer integer, Integer t1) throws Exception {

                                        integers.add(t1);
                                        return integers.size();
                                    }
                                });
                        //        .aggregate(new AggregateFunction<DebeziumStruct, Integer, Integer>() {
                        //            //创建累加器 初始化 最开始这个uv和pv的数值 一般默认是0。但是如果有的公司要作假可以加大。
                        //            @Override
                        //            public Integer createAccumulator() {
                        //                return 0;
                        //            }
                        //            //数据增加逻辑
                        //            @Override
                        //            public Integer add(DebeziumStruct debeziumStruct, Integer o) {
                        //
                        //                return null;
                        //            }
                        //            // 根据 accumulator  计算结果
                        //            @Override
                        //            public Integer getResult(Integer o) {
                        //                return null;
                        //            }
                        //
                        //            @Override
                        //            public Integer merge(Integer o, Integer acc1) {
                        //                return null;
                        //            }
                        //        });


        dealerNum.print();
        env.execute();

//        // 1 source
//        DataStream<PaymentBooksDTO> mainStream = env
//                .addSource(KafkaInfo.getSource())
////               总体过滤
//                .filter(new FilterFunction<ObjectNode>() {
//                    @Override
//                    public boolean filter(ObjectNode value) throws Exception {
//                        return Filter.totalFilter(value);
//                    }
//                })
//                .map(new MapFunction<ObjectNode, PaymentBooksDTO>() {
//                    @Override
//                    public PaymentBooksDTO map(ObjectNode jsonNodes) throws Exception {
//                        return new PaymentBooksDTO(jsonNodes);
//                    }
//                })
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<PaymentBooksDTO>(Time.seconds(0)) {
//                    @Override
//                    public long extractTimestamp(PaymentBooksDTO  paymentBooksDTO) {
//                        return paymentBooksDTO.getDealTime();
//                    }
//                });
////        mainStream.print();
//
////        过滤食堂流
//        DataStream <PaymentBooksDTO> canteenStream  = mainStream
//                .filter(new FilterFunction<PaymentBooksDTO>() {
//                            @Override
//                            public boolean filter(PaymentBooksDTO paymentBooksDTO) throws Exception {
//
//                                if (paymentBooksDTO.getDealerNum() == 1003 || paymentBooksDTO.getDealerNum() == 1009) {
//                                    return false;
//                                }
//                                return true;
//                            }
//                        }
//                );
////        canteenStream.print();
//
////        过滤超市流
//        DataStream <PaymentBooksDTO>  supermarketStream = mainStream
//                .filter(new FilterFunction<PaymentBooksDTO>() {
//                            @Override
//                            public boolean filter(PaymentBooksDTO paymentBooksDTO) throws Exception {
//
//
//                                if (paymentBooksDTO.getDealerNum() == 1003 || paymentBooksDTO.getDealerNum() == 1009) {
//                                    return true;
//                                }else {
//                                    return false;
//                                }
//                            }
//                        }
//
//                );
//
////        supermarketStream.print();
//
//        //doConsumeNumCalc(canteenStream,supermarketStream);
//        //时段消费
//        env.execute("olap_time_consumption");
//
//    }

//    private static void doConsumeNumCalc(DataStream<PaymentBooksDTO> canteenStream,DataStream<PaymentBooksDTO> supermarketStream) {
//
//
////        交易时间
//        DataStream<Tuple2<String,Long>> timeStream =  canteenStream
//                .map(new MapFunction<PaymentBooksDTO, Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> map(PaymentBooksDTO paymentBooksDTO) throws Exception {
//                        return new Tuple2<>("time",paymentBooksDTO.getDealTime());
//                    }
//                });
//
//
////早
//        DataStream<Tuple2<String, Integer>> breakfastStream = canteenStream
//                .filter(new FilterFunction<PaymentBooksDTO>() {
//                    @Override
//                    public boolean filter(PaymentBooksDTO paymentBooksDTO) throws Exception {
//                        if (TimeJudge.nine >= paymentBooksDTO.getDealTime() && TimeJudge.six <= paymentBooksDTO.getDealTime()) {
//                            return true;
//                        } else {
//                            return false;
//                        }
//                    }
//                })
//                .windowAll(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
//                .trigger(new OneByOneTrigger())
//                .apply(new AllWindowFunction<PaymentBooksDTO, Tuple2<String, Integer>, TimeWindow>() {
//                    @Override
//                    public void apply(TimeWindow window, Iterable<PaymentBooksDTO> values, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        HashSet<Integer> idSet = new HashSet<>();
//                        for (PaymentBooksDTO ac : values) {
//                            idSet.add(ac.getAccNum());
//                        }
//                        out.collect(new Tuple2<>("morning", idSet.size()));
//                    }
//                });
//
////        breakfastStream.print();
//
//
////        中午
//
//
//
//        DataStream<Tuple2<String, Integer>> lunchStream = canteenStream
//                .filter(new FilterFunction<PaymentBooksDTO>() {
//                    @Override
//                    public boolean filter(PaymentBooksDTO paymentBooksDTO) throws Exception {
//                        if (TimeJudge.thirteen >= paymentBooksDTO.getDealTime() && TimeJudge.eleven <= paymentBooksDTO.getDealTime()) {
//                            return true;
//                        } else {
//                            return false;
//                        }
//                    }
//                })
//                .windowAll(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
//                .trigger(new OneByOneTrigger())
//                .apply(new AllWindowFunction<PaymentBooksDTO, Tuple2<String, Integer>, TimeWindow>() {
//                    @Override
//                    public void apply(TimeWindow window, Iterable<PaymentBooksDTO> values, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        Long st = window.getStart();
//                        Long et = window.getEnd();
//                        HashSet<Integer> idSet = new HashSet<>();
//                        for (PaymentBooksDTO ac : values) {
//                            idSet.add(ac.getAccNum());
//                        }
//                        out.collect(new Tuple2<>("noon", idSet.size()));
//                    }
//                });
//
//
//
//
////        lunchStream.print();
//
////        晚
//        DataStream<Tuple2<String, Integer>> dinnerStream = canteenStream
//                .filter(new FilterFunction<PaymentBooksDTO>() {
//                            @Override
//                            public boolean filter(PaymentBooksDTO paymentBooksDTO) throws Exception {
//                                if (TimeJudge.nineteen >= paymentBooksDTO.getDealTime() && TimeJudge.sixteen <= paymentBooksDTO.getDealTime()) {
//                                    return true;
//                                } else {
//                                    return false;
//                                }
//                            }
//                        }
//                )
//                .windowAll(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
//                .trigger(new OneByOneTrigger())
//                .apply(new AllWindowFunction<PaymentBooksDTO, Tuple2<String, Integer>, TimeWindow>() {
//                    @Override
//                    public void apply(TimeWindow window, Iterable<PaymentBooksDTO> values, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        HashSet<Integer> idSet = new HashSet<>();
//                        for (PaymentBooksDTO ac : values) {
//                            idSet.add(ac.getAccNum());
//                        }
//                        out.collect(new Tuple2<>("night", idSet.size()));
//                    }
//                });
////        dinnerStream.print();
//
//
////       超市 人数
//        DataStream<Tuple2<String, Integer>> supermarketNumStream = supermarketStream
//                .windowAll(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
//                .trigger(new OneByOneTrigger())
//                .apply(new AllWindowFunction<PaymentBooksDTO, Tuple2<String, Integer>, TimeWindow>() {
//                    @Override
//                    public void apply(TimeWindow window, Iterable<PaymentBooksDTO> values, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        HashSet<Integer> idSet = new HashSet<>();
//                        for (PaymentBooksDTO ac : values) {
//                            idSet.add(ac.getAccNum());
//                        }
//                        out.collect(new Tuple2<>("market", idSet.size()));
//                    }
//                });
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
