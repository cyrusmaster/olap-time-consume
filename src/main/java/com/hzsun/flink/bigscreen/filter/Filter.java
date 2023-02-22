package com.hzsun.flink.bigscreen.filter;


import com.hzsun.flink.bigscreen.kafka.DebeziumStruct;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;
import java.util.Map;

/**
 * @Description  dml filter   针对 debezium MSSQL
 * @ClassName   Filter
 * @Date  2021/9/8 11:09
 * @Author   chenyongfeng
 * @ 遇事不决量子力学
 */
public class Filter {


    private static String [] feeNumArr = new String[]{"50","98","100"};
    private static String [] dealerNumArr = new String[]{"1001","1002","1004","1007","1008","1015","1003","1009"};



    public static boolean totalFilter(ObjectNode value){

        if (value.isEmpty()){
            return false;
        }

        //过滤非插入数据   测试：并未发现快照读r debezium默认不开启快照
        String opStyle = value.get("op").toString().replace("\"", "");
        if (!"c".equals(opStyle)) {
            return false;
        }
        //流水标志  只取1 交易成功
        JsonNode afterData = value.get("after");
        String  recFlag = afterData.get("RecFlag").toString().replace("\"", "");
        if (!"1".equals(recFlag)){
            return false;
        }
        //交易类型 只要 "50","98","100"  ？貌似是说有消费类型
        String feeNum = afterData.get("FeeNum").toString().replace("\"", "");
        if(!Arrays.asList(feeNumArr).contains(feeNum)){
            return false;
        }
        //商户账号   过滤不计算的食堂与超市
        String dealerNum = afterData.get("DealerNum").toString().replace("\"", "");
        if (!Arrays.asList(dealerNumArr).contains(dealerNum)){
            return false;
        }

////        判断今天的数据
//        Long time = Long.valueOf(afterData.get("DealTime").toString().replace("\"", ""));
//        if (time < TimestampsUtils.getTodayZeroPointTimestamps()) {
//            return false;
//        }

        return true;
    }


    public static boolean debeFilter(DebeziumStruct value) {


        if (value == null ){
            return false;
        }

        //过滤非插入数据   测试：并未发现快照读r debezium默认不开启快照
        if (!"c".equals(value.getOp())) {
            return false;
        }
        //流水标志  只取1 交易成功
        Map<String, Object> after = value.getAfter();
        if (!("1".equals (after.get("RecFlag").toString()))){
            return false;
        }
        //交易类型 只要 "50","98","100"  ？貌似是说有消费类型
        if(!Arrays.asList(feeNumArr).contains(after.get("FeeNum").toString())){
            return false;
        }
        //商户账号   过滤不计算的食堂与超市
        if (!Arrays.asList(dealerNumArr).contains(after.get("DealerNum").toString())){
            return false;
        }


        return true;
    }
}
