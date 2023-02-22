package com.hzsun.flink.bigscreen.kafka;


import com.hzsun.flink.bigscreen.utils.TimeUtil;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;


 /**
  * REMARK   Kafka Consumer & Kafka Producer   
  * @className   KafkaInfo
  * @date  2023/2/20 15:40
  * @author  cyf  
  */ 
public class KafkaInfo {


    private static FlinkKafkaConsumer<DebeziumStruct> flinkSource;

    private static FlinkKafkaProducer<String> flinkSink;


    public static final String ZOOKEEPER_SOCKET = "192.168.254.157:2181";
    public static final String BOOTSTRAP_SOCKET = "192.168.254.157:9092";

    //public static final String SOURCE_TOPIC = "sqlserver.dbo.ac_PaymentBooks";
    // test
    public static final String SOURCE_TOPIC = "pay_test";
    //{"breakfastNum":1074,"lunchNum":1722,"dinnerNum":926,"supermarketNum":1410}
    public static final String SINK_TOPIC = "bigscreennum";


    static {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect",ZOOKEEPER_SOCKET);
        properties.setProperty("bootstrap.servers",BOOTSTRAP_SOCKET);

        // 默认   getlast
        flinkSource = new FlinkKafkaConsumer<>(SOURCE_TOPIC, new KafkaDeserialization(),properties);
        flinkSource.setStartFromLatest();
        //flinkSource.setStartFromTimestamp(TimeUtil.getTodayZeroL());


        //flinkSource.setStartFromTimestamp(TimestampsUtils.getYestZeroPointL());





        flinkSink = new  FlinkKafkaProducer<>(
                SINK_TOPIC,new FlinkSerialization(SINK_TOPIC),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

    }


    public static FlinkKafkaConsumer<DebeziumStruct> getSource() {
        return flinkSource;
    }


    public static FlinkKafkaProducer<String> getProducer(){
        return flinkSink;
    }
}
