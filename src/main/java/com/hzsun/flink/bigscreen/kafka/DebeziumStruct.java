package com.hzsun.flink.bigscreen.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

 /**
  * REMARK    结构刨析  显然更加优雅  
  * @className   DebeziumStruct
  * @date  2023/2/17 22:05
  * @author  cyf  
  */ 
@Data
//@AllArgsConstructor
//@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumStruct implements Serializable {
    private Map<String, Object> before;
    private Map<String, Object> after;
    private Map<String, Object> source;
    private String op;
    private String ts_ms;



}
