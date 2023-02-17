package com.hzsun.flink.bigscreen.utils;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



 /**
  * REMARK   1.10 水印窗口测试
  * @className   WinTest
  * @date  2023/2/17 9:46
  * @author  cyf
  */
public class WinTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);




		// 1 source
		env.generateSequence(1,10).print();






		env.execute();








	}


}
