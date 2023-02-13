package com.hzsun.flink.bigscreen.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import javax.annotation.Nullable;


/**
 * @Description  Flink输出数据序列化   用于Flink&Producer 传参
 * @ClassName   OutPutKafkaSchema
 * @Date  2021/8/17 20:08
 * @Author   chenyongfeng
 * @ 遇事不决量子力学  
 */ 
public class FlinkSerialization implements KafkaSerializationSchema<String>{

    private String topic;
    
    public FlinkSerialization(String topic) {
        this.topic = topic;
    }
    
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
        return new ProducerRecord<>(topic, element.getBytes());
    }

}
