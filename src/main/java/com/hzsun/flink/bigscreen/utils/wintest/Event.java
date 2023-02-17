package com.hzsun.flink.bigscreen.utils.wintest;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Event {

private int id;

private long time;

private int fee;


}
