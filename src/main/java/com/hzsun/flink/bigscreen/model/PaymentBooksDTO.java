package com.hzsun.flink.bigscreen.model;

import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

/**
 * @Description  表pojo
 * @ClassName   PaymentBooksDTO
 * @Date  2021/9/8 10:04
 * @Author   chenyongfeng
 * @ 遇事不决量子力学
 */

 @AllArgsConstructor
public class PaymentBooksDTO {

//    卡户账号
    private Integer accNum;
//    交易类型         过滤消费交易
    private Integer feeNum;
//    商户账号    过滤商户类型
    private Integer dealerNum;
//    交易时间
    private  Long dealTime;
//流水标志               确定有效
    private Integer recFlag;

    private LocalDateTime time;






//    public PaymentBooksDTO(JsonNode jsonNodes) {
//        JsonNode dataJson =  jsonNodes.get("after");
//
//        this.accNum = Integer.valueOf(dataJson.get("AccNum").toString().replace("\"", ""));
//        this.feeNum = Integer.valueOf(dataJson.get("FeeNum").toString().replace("\"", ""));
//        this.dealerNum = Integer.valueOf(dataJson.get("DealerNum").toString().replace("\"", ""));
////         已经是北京时区的时间戳！！！  网站转换会多8h   得到真实需要减掉8h
//        this.dealTime = Long.valueOf(dataJson.get("DealTime").toString().replace("\"", "")) ;
//        this.recFlag = Integer.valueOf(dataJson.get("RecFlag").toString().replace("\"", ""));
//    }


    @Override
    public String toString() {
        return "{" +
                "\"accNum\":" + accNum +
                ",\"feeNum\":" + feeNum +
                ",\"dealerNum\":" + dealerNum +
                ",\"dealTime\":" + dealTime +
                ",\"recFlag\":" + recFlag +
                '}';
    }


    public Integer getAccNum() {
        return accNum;
    }

    public void setAccNum(Integer accNum) {
        this.accNum = accNum;
    }

    public Integer getFeeNum() {
        return feeNum;
    }

    public void setFeeNum(Integer feeNum) {
        this.feeNum = feeNum;
    }

    public Integer getDealerNum() {
        return dealerNum;
    }

    public void setDealerNum(Integer dealerNum) {
        this.dealerNum = dealerNum;
    }

    public  Long getDealTime() {
        return dealTime;
    }

    public void setDealTime(Long dealTime) {
        this.dealTime = dealTime;
    }

    public Integer getRecFlag() {
        return recFlag;
    }

    public void setRecFlag(Integer recFlag) {
        this.recFlag = recFlag;
    }
}
