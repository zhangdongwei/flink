package com.spursgdp.flink.streaming.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 未开启checkpoint，通过配置自动提交让Kafka自己来维护消费端的offset，不推荐
 */
public class StreamingKafkaSourceDemo1 {

    public static void main(String[] args) throws Exception {

        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka配置项
        String topic = "t1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","ubuntu:9092");
        prop.setProperty("group.id","group1");  //消费者组id

        //初始化consumer
        FlinkKafkaConsumer011<String> flinkKafkaConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);
        flinkKafkaConsumer.setStartFromGroupOffsets();  //默认的消费策略

        //source
        DataStreamSource<String> source = env.addSource(flinkKafkaConsumer);

        //直接输出
        source.print().setParallelism(1);

        //execute
        env.execute("StreamingKafkaSource1");

    }


}
