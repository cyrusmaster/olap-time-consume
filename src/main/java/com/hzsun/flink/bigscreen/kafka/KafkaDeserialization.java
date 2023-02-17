package com.hzsun.flink.bigscreen.kafka;


import com.hzsun.flink.bigscreen.utils.TimestampsUtils;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description   Kafka数据反序列化  用于Consume&Flink 传参
 * @ClassName   KafkaDeserialization
 * @Date  2021/8/18 20:13
 * @Author   chenyongfeng
 * @ 遇事不决量子力学
 */
//@PublicEvolving
//@JsonIgnoreProperties(ignoreUnknown = true)

public class KafkaDeserialization extends AbstractDeserializationSchema<DebeziumStruct> {

    private static final long serialVersionUID = -1699854177598621044L;

    private final ObjectMapper mapper = new ObjectMapper();
    //private final DebeziumStruct debeziumStruct = new DebeziumStruct();

    @Override
    public DebeziumStruct deserialize(byte[] message) throws IOException {
        //String result = null;
        DebeziumStruct result = null;
        try {
            result = mapper.readValue(message, DebeziumStruct.class);

            // eventTime-8 处理
            Object dealTime = result.getAfter().get("DealTime");
            Long dealTime1 = (Long) dealTime;
            Long subtract8hTimestamp = TimestampsUtils.getSubtract8hTimestamp(dealTime1);

            Map<String,Object> map = result.getAfter();
            map.replace("DealTime",subtract8hTimestamp);
            result.setAfter(map);



        } catch (Exception e) {
            message = "{\"op\": \"no\"}\n".getBytes();
            //result = new String(message);
            result = mapper.readValue(message, DebeziumStruct.class);

        }
        return result;
    }

}