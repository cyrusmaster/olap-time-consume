package com.hzsun.flink.bigscreen.utils;

public class TimeJudge {


//    6 、9、11、13、16、19
    static Long sixTimestamps = Long.valueOf(60 * 60 * 6* 1000);
    static Long nineTimestamps = Long.valueOf(60 * 60 * 9 * 1000);
    static Long elevenTimestamps = Long.valueOf(60 * 60 * 11 * 1000);
    static Long thirteenTimestamps = Long.valueOf(60 * 60 * 13 * 1000);
    static Long sixteenTimestamps = Long.valueOf(60 * 60 * 16 * 1000);
    static Long nineteenTimestamps = Long.valueOf(60 * 60 * 19 * 1000);





    public  static Long six = TimeUtil.getTodayZeroPointTimestamps() + sixTimestamps;
    public  static Long nine = TimeUtil.getTodayZeroPointTimestamps() + nineTimestamps;
    public  static Long eleven = TimeUtil.getTodayZeroPointTimestamps() + elevenTimestamps;
    public  static Long thirteen = TimeUtil.getTodayZeroPointTimestamps() + thirteenTimestamps;
    public  static Long sixteen = TimeUtil.getTodayZeroPointTimestamps() + sixteenTimestamps;
    public  static Long nineteen = TimeUtil.getTodayZeroPointTimestamps() + nineteenTimestamps;


    public static void main(String[] args) {
        // 14    17
        System.out.println(six +" "+nine);


    }

}






