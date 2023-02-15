package com.hzsun.flink.bigscreen;

import com.hzsun.flink.bigscreen.dto.ConsumeNumDTO;
import com.hzsun.flink.bigscreen.dto.PaymentBooksDTO;
import com.hzsun.flink.bigscreen.filter.Filter;
import com.hzsun.flink.bigscreen.kafka.DebeziumStruct;
import com.hzsun.flink.bigscreen.kafka.KafkaInfo;
import com.hzsun.flink.bigscreen.trigger.OneByOneTrigger;
import com.hzsun.flink.bigscreen.utils.TimeJudge;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.util.HashSet;


/**
 * @Description  时段消费
 * @ClassName   WaterJob
 * @Date  2021/8/26 10:11
 * @Author   chenyongfeng
 * @ 遇事不决量子力学
 */
public class FlinkJob {
    // todo 测试日志
    //private static final Logger LOG = LoggerFactory.getLogger(FlinkJob.class);


    public static void main(String[] args) throws Exception{

        // 1定义环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env .setParallelism(1);
        // 10版本中 streaming programs need to set the time characteristic accordingly.
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2 source -> event stream
        //ObjectMapper objectMapper = new ObjectMapper();
        SingleOutputStreamOperator<DebeziumStruct> filter = env
        .addSource(KafkaInfo.getSource())
                //.map( value -> objectMapper.readValue(value,DebeziumStruct.class))
                .filter(Filter::debeFilter)
                //.map(v -> new PaymentBooksDTO((Integer)v.getAfter().get("AccNum"),(Integer)v.getAfter().get("FeeNum"),
                //(Integer)v.getAfter().get("DealerNum"),))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<DebeziumStruct>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(DebeziumStruct debeziumStruct) {

                        //if (debeziumStruct.getAfter().get("DealTime") != null) {
                        //
                        //}
                        //String string = String.valueOf(debeziumStruct.getAfter().get("DealTime"));
                        //long l = 1676422374000L;
                        return (long)debeziumStruct.getAfter().get("DealTime") ;
                    }
                })


                        ;



        filter.print();
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
