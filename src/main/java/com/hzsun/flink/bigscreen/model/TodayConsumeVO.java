package com.hzsun.flink.bigscreen.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * @Description  用水
 * @ClassName   WaterDTO
 * @Date  2021/8/31 10:53
 * @Author   chenyongfeng
 * @ 遇事不决量子力学
 */


@Data
public class TodayConsumeVO implements Serializable {


    private  Integer breakfastNum;
    private  Integer lunchNum;
    private  Integer dinnerNum;
    private  Integer supermarketNum;

    private  Long eventTime;



    public TodayConsumeVO() {
        this.breakfastNum = 0;
        this.lunchNum = 0;
        this.dinnerNum = 0;
        this.supermarketNum = 0;
        this.eventTime = 0L;
    }

    @Override
    public String toString() {

        return "{" +
                "\"breakfastNum\":" + breakfastNum +
                ",\"lunchNum\":" + lunchNum +
                ",\"dinnerNum\":" + dinnerNum +
                ",\"supermarketNum\":" + supermarketNum +
                '}';
    }


}

