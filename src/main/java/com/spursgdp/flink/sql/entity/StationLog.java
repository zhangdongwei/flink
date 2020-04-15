package com.spursgdp.flink.sql.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * 基站日志
 *
 *  sid 基站的id
 *  callOut 主叫号码
 *  callInt 被叫号码
 *  callType 呼叫类型
 *  callTime 呼叫时间 (毫秒)
 *  duration 通话时长 （秒）
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class StationLog {

    private String sid;

    private String callOut;

    private String callInt;

    private String callType;

    private Long callTime;

    private Long duration;
}
