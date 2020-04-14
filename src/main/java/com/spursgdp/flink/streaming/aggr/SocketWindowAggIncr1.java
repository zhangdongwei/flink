package com.spursgdp.flink.streaming.aggr;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zhangdongwei
 * @create 2020-04-14-14:33
 */
public class SocketWindowAggIncr1 {

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
        source.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        })
                //滚动窗口，窗口大小=5s，由于当前不是一个keyedStream，所以不能使用timeWindow方法
                .timeWindowAll(Time.seconds(5))
//              .sum(0)  //sum和下面的reduce等价
                .reduce(new ReduceFunction<Integer>() {
                    @Override
                    public Integer reduce(Integer i1, Integer i2) throws Exception {
                        System.out.println("执行reduce操作："+i1+","+i2);
                        return i1+i2;
                    }
                })
                .print();

        //启动env
        env.execute("SocketWindowAggIncr1");



    }

}
