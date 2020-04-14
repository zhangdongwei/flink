package com.spursgdp.flink.streaming.aggr;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhangdongwei
 * @create 2020-04-14-14:52
 */
public class SocketWindowAggFull1 {

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
        source.timeWindowAll(Time.seconds(5))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<String> inputs, Collector<String> out) throws Exception {
                        long count = 0;
                        for (String input : inputs) {
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
