package com.spursgdp.flink.streaming.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * Kafka Sink：开启checkpoint + exactly once语义，推荐
 */
public class StreamingKafkaSinkDemo2 {

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


        //source
        DataStreamSource<String> source = env.fromElements("a","b","c","d","e","f","g","h");

        //Kafka配置
        String brokerList = "ubuntu:9092";
        String topic = "t1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);
        //第一种解决方案，设置FlinkKafkaProducer011里面的事务超时时间
        prop.setProperty("transaction.timeout.ms",60000*15+"");  //15分钟
        //第二种解决方案，修改Kafka的配置，设置kafka的最大事务超时时间(transaction.max.timeout.ms)为1小时


        //初始化FlinkKafkaProducer
//      FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(brokerList, topic, new SimpleStringSchema());
        //使用仅一次语义的kafkaProducer
        FlinkKafkaProducer011<String> myProducer =
                new FlinkKafkaProducer011<>(topic,
                                            new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
                                            prop,
                                            FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);


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
