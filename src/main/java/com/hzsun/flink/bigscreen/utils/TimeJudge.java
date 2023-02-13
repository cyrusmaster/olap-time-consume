package com.hzsun.flink.bigscreen.utils;

public class TimeJudge {


//    6 、9、11、13、16、19
    static Long sixTimestamps = Long.valueOf(60 * 60 * 6* 1000);
    static Long nineTimestamps = Long.valueOf(60 * 60 * 9 * 1000);
    static Long elevenTimestamps = Long.valueOf(60 * 60 * 11 * 1000);
    static Long thirteenTimestamps = Long.valueOf(60 * 60 * 13 * 1000);
    static Long sixteenTimestamps = Long.valueOf(60 * 60 * 16 * 1000);
    static Long nineteenTimestamps = Long.valueOf(60 * 60 * 19 * 1000);





    public  static Long six = TimestampsUtils.getTodayZeroPointTimestamps() + sixTimestamps;
    public  static Long nine = TimestampsUtils.getTodayZeroPointTimestamps() + nineTimestamps;
    public  static Long eleven = TimestampsUtils.getTodayZeroPointTimestamps() + elevenTimestamps;
    public  static Long thirteen = TimestampsUtils.getTodayZeroPointTimestamps() + thirteenTimestamps;
    public  static Long sixteen = TimestampsUtils.getTodayZeroPointTimestamps() + sixteenTimestamps;
    public  static Long nineteen = TimestampsUtils.getTodayZeroPointTimestamps() + nineteenTimestamps;


    public static void main(String[] args) {
        // 14    17
        System.out.println(six +" "+nine);


    }

}






