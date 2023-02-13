package com.hzsun.flink.bigscreen.filter;


import com.hzsun.flink.bigscreen.utils.TimestampsUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * @Description  主流过滤     debeziun  读 sql server cdc 版
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

//        过滤非插入数据
        String opStyle = value.get("op").toString().replace("\"", "");
        if (!"c".equals(opStyle)) {
            return false;
        }
//        处理标记判断  只取交易成功
        JsonNode afterData = value.get("after");
        String  recFlag = afterData.get("RecFlag").toString().replace("\"", "");
        if (!"1".equals(recFlag)){
            return false;
        }
//        过滤消费数据
        String feeNum = afterData.get("FeeNum").toString().replace("\"", "");
        if(!Arrays.asList(feeNumArr).contains(feeNum)){
            return false;
        }
//        过滤食堂与超市
        String dealerNum = afterData.get("DealerNum").toString().replace("\"", "");
        if (!Arrays.asList(dealerNumArr).contains(dealerNum)){
            return false;
        }
//        判断今天的数据     
        Long time = Long.valueOf(afterData.get("DealTime").toString().replace("\"", ""));
        if (time < TimestampsUtils.getTodayZeroPointTimestamps()) {
            return false;
        }

        return true;
    }
}
