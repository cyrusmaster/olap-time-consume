package com.hzsun.flink.bigscreen.utils;

import java.text.SimpleDateFormat;
import java.time.*;

public class TimeUtil {




    // 是否相同天

    public static boolean isSameDay(long l1 , long l2){


        LocalDate localDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(l1), ZoneOffset.UTC).toLocalDate();
        LocalDate localDate2 = LocalDateTime.ofInstant(Instant.ofEpochMilli(l2), ZoneOffset.UTC).toLocalDate();

        return localDate.equals(localDate2);
    }

    /*
     * REMARK    时间戳转换工具  等同  https://tool.lu/timestamp/
     * @methodName   timeStampToTime
     * @return java.lang.String
     * @date 2023/2/16 19:54
     * @author cyf
     */
    public static String timeStampToTime(long l){

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return l+"|"+simpleDateFormat.format(l) ;

    }





    /*
     * REMARK  当日8点 用来 判断三餐   弃用
     * @methodName   getTodayZeroPointTimestamps
     * @return java.lang.Long
     * @date 2023/2/16 19:11
     * @author cyf
     */
     @Deprecated
    public static Long getTodayZeroPointTimestamps() {
        Long currentTimestamps = System.currentTimeMillis();
        Long oneDayTimestamps = Long.valueOf(60 * 60 * 24 * 1000);
        Long result = currentTimestamps - (currentTimestamps) % oneDayTimestamps;
        return result;
    }
    
    
    
    /*
     * REMARK  今日0点时间戳  用于 stream kafka读取偏移量
     * https://tool.lu/timestamp/    2023-02-16 00:00:00
     * @methodName   getTimestamps
     * @return java.lang.Long
     * @date 2023/2/16 19:13
     * @author cyf
     */
    public static  Long getTodayZeroL(){
        long currentTimestamps = System.currentTimeMillis();
        long oneDayTimestamps = 60 * 60 * 24 * 1000;
        long eightTimestamps = 60 * 60 * 8 * 1000;
        long result = currentTimestamps - (currentTimestamps) % oneDayTimestamps;
        return result-eightTimestamps;
    }

    
    /*
     * REMARK  昨日零点   用于 stream kafka  因为有迟到数据
     * @methodName   getYestZeroPointL
     * @return long
     * @date 2023/2/16 19:31
     * @author cyf
     */
    public static long getYestZeroPointL() {
        long now = System.currentTimeMillis();
        long daySeconds = 1000 * 60 * 60 * 24;
        // 东八区加八小时
        // 获取零点时间戳
        long t = now - (now + 8 * 60 * 60 * 1000) % daySeconds;
        return t-daySeconds;
    }

    /*
     * REMARK  今日 0点   用于 table kafka
     * @methodName   getTodayZeroPointS
     * @return java.lang.String
     * @date 2023/2/16 19:32
     * @author cyf
     */
    public static String getTodayZeroPointS() {
        long now = System.currentTimeMillis();
        long daySeconds = 1000 * 60 * 60 * 24;
        // 东八区加八小时
        // 获取零点时间戳
        long t = now - (now + 8 * 60 * 60 * 1000) % daySeconds;

        String s = String.valueOf( now - (now + 8 * 60 * 60 * 1000) % daySeconds);
        return s;
    }

    /*
     * REMARK  当前时间戳减8h 方法 用于事件时间多8h
     * @methodName   getTrueTimestamp
     * @return java.lang.Long
     * @date 2023/2/16 19:35
     * @author cyf
     */
   public static Long getSubtract8hTimestamp(String s){

        //before
        Long aLong = Long.valueOf(s);
        //after
        long eightSeconds = 1000 * 60 * 60 * 8;
        Long now = aLong - eightSeconds;

        return  now;
    }

    /*
     * REMARK   -8  拿到真实事件(同上)
     * @methodName   getSubtract8hTimestamp
     * @return java.lang.Long
     * @date 2023/2/16 20:53
     * @author cyf
     */
  public static Long getSubtract8hTimestamp(long s){


        //after
        long eightSeconds = 1000 * 60 * 60 * 8;
        Long now = s - eightSeconds;

        return  now;
    }


    /*
     * REMARK  时间戳long计算
     * @methodName   subtract8h
     * @return long
     * @date 2023/2/21 15:01
     * @author cyf
     */
    public static long calTimestamp  (long l,int i){
        ZonedDateTime utc = Instant.ofEpochMilli(l).atZone(ZoneId.of("UTC"));
        return utc.plusHours(i).toInstant().toEpochMilli();
    }

    /*
     * REMARK   造时间+8  测试用
     * @methodName   plus8h
     * @return java.lang.Long
     * @date 2023/2/16 20:55
     * @author cyf
     */
    public static Long plus8h(long l){
        long eightSeconds = 1000 * 60 * 60 * 8;
        Long now = l + eightSeconds;
        return  now;
    }



    public static void main(String[] args) {
        //System.out.println(plus8h(1676595522000l)
        //
        //
        //);

        //java
        //System.out.println(Duration.ofSeconds(3));
        //flink api
        //System.out.println(Time.days(1));
        //flinkstram
        //System.out.println(org.apache.flink.streaming.api.windowing.time.Time.days(1));


        //System.out.println(timeStampToTime(calTimestamp(1676960294583L,-8)));

        // debe -> real
        System.out.println(timeStampToTime(calTimestamp(1677064204000L,-8)));
        // create  debe
        System.out.println(calTimestamp(1677113754000L,8));

        //当天
        System.out.println(Instant.now().toEpochMilli());
        System.out.println(LocalDateTime.ofInstant(Instant.ofEpochMilli(1677067273148L), ZoneOffset.UTC).toLocalDate());

        System.out.println(isSameDay(1677060613000L,1677147013000L));

        //1  2023-02-21 14:18:14  比实际多8  我要-8  得到早6点
        //Long l = 1676960294583L;
        //System.out.println(timeStampToTime(l));
        ////2 带时区时间
        //ZonedDateTime utc = Instant.ofEpochMilli(l).atZone(ZoneId.of("UTC"));
        //System.out.println(utc);
        //OffsetDateTime offsetDateTime = Instant.ofEpochMilli(l).atOffset(ZoneOffset.of("+8"));
        //System.out.println(offsetDateTime);
        ////3 时间计算
        //long l1 = utc.plusHours(-8).toInstant().toEpochMilli();
        ////Instant instant1 = offsetDateTime.plusHours(-8).toInstant();
        ////long l1 = instant.toEpochMilli();
        ////long l2 = instant1.toEpochMilli();
        //
        //System.out.println(timeStampToTime(l1));
        ////System.out.println(timeStampToTime(l2));


    }





}
