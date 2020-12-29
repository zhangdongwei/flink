package com.spursgdp.flink.streaming.custom;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 传感器实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Sensor {

    private String sensorId;

    private Long timestamp;

    private Double temprature;

}

