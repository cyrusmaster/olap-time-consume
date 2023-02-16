package com.hzsun.flink.bigscreen.kafka;


import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * @Description   Kafka数据反序列化  用于Consume&Flink 传参
 * @ClassName   KafkaDeserialization
 * @Date  2021/8/18 20:13
 * @Author   chenyongfeng
 * @ 遇事不决量子力学
 */
//@PublicEvolving
//@JsonIgnoreProperties(ignoreUnknown = true)

public class KafkaDeserializationWithString extends AbstractDeserializationSchema<String> {

    private static final long serialVersionUID = -1699854177598621044L;

    private final ObjectMapper mapper = new ObjectMapper();
    //private final DebeziumStruct debeziumStruct = new DebeziumStruct();

    @Override
    public String deserialize(byte[] message) throws IOException {
        String result = null;

        try {
            //result = mapper.readValue(message, ObjectNode.class);
            result = new String(message);

        } catch (Exception e) {
            message = "{\"op\": \"no\"}\n".getBytes();
            result = new String(message);


        }
        return result;
    }

}