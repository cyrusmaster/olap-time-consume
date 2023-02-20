package com.hzsun.flink.bigscreen.model;

import java.io.Serializable;

/**
 * @Description  用水
 * @ClassName   WaterDTO
 * @Date  2021/8/31 10:53
 * @Author   chenyongfeng
 * @ 遇事不决量子力学
 */
public class TodayConsumeVO implements Serializable {


    private  Integer breakfastNum;
    private  Integer lunchNum;
    private  Integer dinnerNum;
    private  Integer supermarketNum;
    //private Long  time;
    //private Long  starttime;
    //private Long  endtime;



    public TodayConsumeVO() {
        this.breakfastNum = 0;
        this.lunchNum = 0;
        this.dinnerNum = 0;
        this.supermarketNum = 0;

    }

    @Override
    public String toString() {

        return "{" +
                "\"breakfastNum\":" + breakfastNum +
                ",\"lunchNum\":" + lunchNum +
                ",\"dinnerNum\":" + dinnerNum +
                ",\"supermarketNum\":" + supermarketNum +
//                ",\"time\":" +Flin st +
//                ",\"time\":" + time +
                '}';
    }


    public Integer getBreakfastNum() {
        return breakfastNum;
    }

    public void setBreakfastNum(Integer breakfastNum) {
        this.breakfastNum = breakfastNum;
    }

    public Integer getLunchNum() {
        return lunchNum;
    }

    public void setLunchNum(Integer lunchNum) {
        this.lunchNum = lunchNum;
    }

    public Integer getDinnerNum() {
        return dinnerNum;
    }

    public void setDinnerNum(Integer dinnerNum) {
        this.dinnerNum = dinnerNum;
    }

    public Integer getSupermarketNum() {
        return supermarketNum;
    }

    public void setSupermarketNum(Integer supermarketNum) {
        this.supermarketNum = supermarketNum;
    }

    //public Long getTime() {
    //    return time;
    //}
    //
    //public void setTime(Long time) {
    //    this.time = time;
    //}
}

