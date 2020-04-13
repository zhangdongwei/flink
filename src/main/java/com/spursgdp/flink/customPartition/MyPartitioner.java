package com.spursgdp.flink.customPartition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author zhangdongwei
 * @create 2020-04-05-21:12
 */
public class MyPartitioner implements Partitioner<Long> {

    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("分区总数：" + numPartitions);
        if(key % 2 == 0){
            return 0;
        }else{
            return 1;
        }
    }
}
