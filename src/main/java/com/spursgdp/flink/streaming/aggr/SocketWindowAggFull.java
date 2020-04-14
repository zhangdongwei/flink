package com.spursgdp.flink.streaming.aggr;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhangdongwei
 * @create 2020-04-14-14:52
 */
public class SocketWindowAggFull {

    public static void main(String[] args) throws Exception {

        System.out.println("Java Socket Window Count...");
        //获取需要的端口号，如不输入默认给9001
        int port;
        try {
            port = ParameterTool.fromArgs(args).getInt("port");
        }catch (Exception e){
            port = 9001;
        }

        //创建Flink对应的Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String host = "ubuntu";
        String delimiter = "\n";    //一行为一条记录

        //source
        DataStreamSource<String> source = env.socketTextStream(host, port, delimiter);

        //transform
        source.map(new MapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(String value) throws Exception {
                return new Tuple2<>(1,value);
            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(5))  //滚动窗口，窗口大小=5s
                .process(new ProcessWindowFunction<Tuple2<Integer, String>, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, String>> inputs, Collector<String> out) throws Exception {
                        long count = 0;
                        for (Tuple2<Integer, String> input : inputs) {
                            count++;
                        }
                        out.collect("window: " + context.window() + ",count: " + count);
                    }
                })
                .print();

        //启动env
        env.execute("SocketWindowAggFull");
    }

}
