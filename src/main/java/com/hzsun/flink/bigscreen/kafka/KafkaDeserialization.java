package com.hzsun.flink.bigscreen.kafka;


import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/**
 * @Description   Kafka数据反序列化  用于Consume&Flink 传参
 * @ClassName   KafkaDeserialization
 * @Date  2021/8/18 20:13
 * @Author   chenyongfeng
 * @ 遇事不决量子力学
 */
@PublicEvolving
public class KafkaDeserialization extends AbstractDeserializationSchema<ObjectNode> {

    private static final long serialVersionUID = -1699854177598621044L;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public ObjectNode deserialize(byte[] message) throws IOException {
        ObjectNode result = null;
        try {
            result = mapper.readValue(message, ObjectNode.class);
        } catch (Exception e) {
            message = "{\"op\": \"no\"}\n".getBytes();
            result = mapper.readValue(message, ObjectNode.class);
        }
        return result;
    }

}