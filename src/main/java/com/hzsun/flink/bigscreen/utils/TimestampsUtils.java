package com.hzsun.flink.bigscreen.utils;

public class TimestampsUtils {

    //      零点
    public static Long getTodayZeroPointTimestamps() {
        Long currentTimestamps = System.currentTimeMillis();
//        一天的时间戳
        Long oneDayTimestamps = Long.valueOf(60 * 60 * 24 * 1000);
//        今天零点的时间戳= 现在时间戳-（今天零点到现在的时间戳）
//        当前时区  今天零点到现在的时间戳 = 当先时区时间戳 取余  一天时间戳
//           因为GTM时区需要加8
//        gmt 0点     适合 oracle kafka connector  生成时间 +8 才是北京时区零点
//        Long result = currentTimestamps - (currentTimestamps + 60 * 60 * 8 * 1000) % oneDayTimestamps;


//       北京  0点   适合 debezium cdc生成的时间戳  （时间戳转换工具会加八 当作gmt时间戳处理 实际显示8点）
        Long result = currentTimestamps - (currentTimestamps) % oneDayTimestamps;
        return result;
    }

    public static  Long getTimestamps(){
        Long currentTimestamps = System.currentTimeMillis();
        Long oneDayTimestamps = Long.valueOf(60 * 60 * 24 * 1000);
        Long eightTimestamps = Long.valueOf(60 * 60 * 8 * 1000);
        Long result = currentTimestamps - (currentTimestamps) % oneDayTimestamps;
        return result-eightTimestamps;



    }

    public static void main(String[] args) {
        System.out.println(getTodayZeroPointTimestamps()
        );
    }





}
