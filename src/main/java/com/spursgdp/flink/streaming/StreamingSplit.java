package com.spursgdp.flink.streaming;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author zhangdongwei
 * @create 2020-04-05-20:07
 */
public class StreamingSplit {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            list.add(i);
        }
        DataStreamSource<Integer> source = env.fromCollection(list);

        SplitStream<Integer> splitStream = source.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                ArrayList<String> l = new ArrayList<>();
                if (value % 2 == 0) {
                    l.add("even");   // 偶数
                } else {
                    l.add("odd");    // 奇数
                }
                return l;
            }
        });

        DataStream<Integer> evenStream = splitStream.select("even");
        DataStream<Integer> oddStream = splitStream.select("odd");

//        evenStream.print().setParallelism(1);
        oddStream.print().setParallelism(1);

        env.execute("Streaming Split Demo...");

    }
}
