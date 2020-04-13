package com.spursgdp.flink.batch;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * @author zhangdongwei
 * @create 2020-04-06-11:29
 */
public class DataSetMapPartitionDemo {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //source
        List<String> data = Arrays.asList("hello spark", "hello flink", "hello hadoop", "hello hadoop flink");

        DataSource<String> source = env.fromCollection(data);

        //transform
//        source.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String value) throws Exception {
//                //获取数据库连接--注意，此时是每过来一条数据就获取一次链接
//                //处理数据
//                //关闭连接
//                return value;
//            }
//        });

        MapPartitionOperator<String, String> MapPartitionData = source.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                //values中保存了一个分区的数据

                //获取数据库连接 -- 注意，此时是一个分区的数据获取一次连接【优点：每个分区获取一次连接】

                //处理数据（可以是数据库的insert操作）
                for (String line : values) {
                    String[] words = line.split("\\W+");
                    for (String word : words) {
                        out.collect(word);
                    }
                }
            }
        });

        MapPartitionData.print();

        //Batch处理可以不加这句
//        env.execute("DataSet Batch MapPartition Demo...");

    }


}

