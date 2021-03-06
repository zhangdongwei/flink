package com.spursgdp.flink.streaming.aggr;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zhangdongwei
 * @create 2020-04-14-14:33
 */
public class SocketWindowAggIncr {

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
        source.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String value) throws Exception {
                return new Tuple2<>(1, Integer.valueOf(value));
            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(5))  //滚动窗口，窗口大小=5s
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) throws Exception {
                        System.out.println("执行reduce操作：" + t1 + "," + t2);
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                })
                .print();

        //启动env
        env.execute("SocketWindowAggIncr");


    }

}
