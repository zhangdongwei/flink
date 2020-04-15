package com.spursgdp.flink.streaming.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 开启checkpoint来维护消费端的offset，推荐
 */
public class StreamingKafkaSourceDemo2 {

    public static void main(String[] args) throws Exception {

        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //checkpoint配置
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //statebackend，提交到集群的时候可以打开该行，如不打开，checkpoint默认存储于内存
        //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop100:9000/flink/checkpoints",true));


        //kafka配置项
        String topic = "t1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","ubuntu:9092");
        prop.setProperty("group.id","group2");  //消费者组id

        //初始化consumer
        FlinkKafkaConsumer011<String> flinkKafkaConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);
        flinkKafkaConsumer.setStartFromGroupOffsets();  //默认的消费策略

        //source
        DataStreamSource<String> source = env.addSource(flinkKafkaConsumer);

        //直接输出
        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                System.out.println("当前线程id："+Thread.currentThread().getId()+",对应数据："+s);  //这里起作用的是两个线程，因为topic的partitions=2
                return s;
            }
        }).print().setParallelism(1);

        //execute
        env.execute("StreamingKafkaSource2");


        //execute
        env.execute("StreamingKafkaSource1");

    }


}
