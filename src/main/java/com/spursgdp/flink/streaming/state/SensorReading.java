package com.spursgdp.flink.streaming.state;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 传感器实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {

    private String id;

    private Long timestamp;

    private Double temprature;

}

