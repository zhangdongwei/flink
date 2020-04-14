package com.spursgdp.flink.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Arrays;
import java.util.List;

/**
 * @author zhangdongwei
 * @create 2020-04-06-10:53
 */
public class StreamingRedisSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> inputs = Arrays.asList("Flink","Hadoop","Spark","Kafka");
        //source
        DataStreamSource<String> source = env.fromCollection(inputs);

        //sink：需要传入一个RedisMapper实现类
        final String key = "l_word";

        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder().setHost("ubuntu").setPort(6379).build();
        RedisSink redisSink = new RedisSink(redisConf, new RedisMapper<String>() {

            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.LPUSH);
            }

            @Override
            public String getKeyFromData(String value) {
                return key;
            }

            @Override
            public String getValueFromData(String value) {
                return value;
            }
        });

        source.addSink(redisSink);

        env.execute("Streaming Redis Sink...");
    }


}
