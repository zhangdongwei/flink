package com.spursgdp.flink.streaming.custom;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义数据源Source，可以用来灵活的生成模拟的线上数据
 * 参考： https://www.bilibili.com/video/BV1qy4y1q728?p=27
 */
public class StreamingCustomSourceDemo {

    public static void main(String[] args) throws Exception {

        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //source
        DataStreamSource<Sensor> source = env.addSource(new MySensorSource());

        //直接输出
        source.print().setParallelism(1);

        //execute
        env.execute("StreamingCustomSourceDemo");

    }

}


