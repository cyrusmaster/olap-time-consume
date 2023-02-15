package com.hzsun.flink.bigscreen.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

@Data
//@AllArgsConstructor
//@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumStruct implements Serializable {
    private Map<String, Object> before;
    private Map<String, Object> after;
    private Map<String, Object> source;
    private String op;



}
