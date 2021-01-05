package com.spursgdp.flink.streaming.custom;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 自定义数据源Source，可以用来灵活的生成模拟的线上数据
 * 参考： https://www.bilibili.com/video/BV1qy4y1q728?p=27
 */
public class StreamingCustomSourceDemo {

    public static void main(String[] args) throws Exception {

        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(500);

        //source
        DataStreamSource<Sensor> source = env.addSource(new MySensorSource());

        source.
        assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Sensor>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(Sensor sensor) {
                return sensor.getTimestamp();
            }
        });

        source.
        assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Sensor>() {
            @Override
            public long extractAscendingTimestamp(Sensor sensor) {
                return sensor.getTimestamp();
            }
        });


        //直接输出
        source.print().setParallelism(1);

        //execute
        env.execute("StreamingCustomSourceDemo");

    }

}


