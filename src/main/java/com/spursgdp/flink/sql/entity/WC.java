package com.spursgdp.flink.sql.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author zhangdongwei
 * @create 2020-04-17-9:28
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class WC {

    private String word;

    private Integer frequence;

}
