package com.spursgdp.flink.customPartition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author zhangdongwei
 * @create 2020-04-05-21:16
 */
public class StreamingWithCustomPartition {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //source
        ArrayList<Long> list = new ArrayList<>();
        for (int i = 1; i <= 6; i++) {
            list.add((long)i);
        }
        DataStreamSource<Long> source = env.fromCollection(list);

        //transform：对数据进行转换，将Long转换成Tuple1，否则无法调用partition方法
        SingleOutputStreamOperator<Tuple1<Long>> tupleDS = source.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1 map(Long value) throws Exception {
                return new Tuple1<Long>(value);
            }
        });

        //自定义重分区
        DataStream<Tuple1<Long>> partitionDS = tupleDS.partitionCustom(new MyPartitioner(), 0);

        //进行map操作，主要是为了输出分区情况
        SingleOutputStreamOperator<Long> resultDS = partitionDS.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> t) throws Exception {
                //打印当前线程
                Long value = t.f0;
                System.out.println("当前线程id：" + Thread.currentThread().getId() + ",value = " + value);
                return value;
            }
        });

        //sink
        resultDS.print().setParallelism(1);

        env.execute("Streaming with custom partition...");

    }
}
