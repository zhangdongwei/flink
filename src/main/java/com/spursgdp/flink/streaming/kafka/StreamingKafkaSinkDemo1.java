package com.spursgdp.flink.streaming.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * Kafka Sink：未开启checkpoint，不推荐
 */
public class StreamingKafkaSinkDemo1 {

    public static void main(String[] args) throws Exception {

        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> source = env.fromElements("a","b","c","d","e","f","g","h");

        //Kafka配置
        String brokerList = "ubuntu:9092";
        String topic = "t1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);

        //初始化FlinkKafkaProducer
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(brokerList, topic, new SimpleStringSchema());

        //sink
        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                System.out.println("当前线程id："+Thread.currentThread().getId()+",对应数据："+s);  //生产端不受Kafka topic partition数限制，4个线程一起插入数据
                return s;
            }
        }).addSink(myProducer);

        //execute
        env.execute("StreamingKafkaSinkDemo1");



    }


}
