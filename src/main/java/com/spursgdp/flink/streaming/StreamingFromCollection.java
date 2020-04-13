package com.spursgdp.flink.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author zhangdongwei
 * @create 2020-04-05-20:07
 */
public class StreamingFromCollection {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));

//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        DataSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));


        SingleOutputStreamOperator<Integer> result = source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer i) throws Exception {
                return i + 1;
            }
        });

        result.setParallelism(1).print();

        env.execute("Streaming Collection Source...");

    }
}
