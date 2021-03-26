package com.spursgdp.flink.streaming.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * 操作算子(Operator State）示例
 * 需求：定义一个有状态的map操作，统计当前每个分区的数据个数
 * @author zhangdongwei
 * @create 2021-01-05-10:01
 */
public class StateTest2_KeyedState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 文件输入流
        DataStream<String> inputStream = env.readTextFile("D:\\projects\\flink\\src\\main\\resources\\sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前key对应的数据个数，所以必须要先进行keyBy操作后才能使用KeyedState
        SingleOutputStreamOperator<String> resultStream = dataStream.keyBy("sensorId").map(new MyKeyedMapFunction());

        dataStream.print("dataStream");
        resultStream.print("resultStream");

        env.execute();
    }

    public static class MyKeyedMapFunction extends RichMapFunction<SensorReading, String> implements ListCheckpointed<Integer> {

        private ValueState<Integer> keyCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化ValueState
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));
        }

        @Override
        public String map(SensorReading sensorReading) throws Exception {
            //读取ValueState的值
            Integer count = keyCountState.value();
            if(count == null) {
                count = 0;
            }
            count++;
            //修改ValueState
            keyCountState.update(count);
            String str = sensorReading.getId() + " : " + count;
            return str;
        }

        @Override
        public void close() throws Exception {
            keyCountState.clear();
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            Integer count = keyCountState.value();
            if(count == null) {
                count = 0;
            }
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            Integer count = 0;
            for (Integer num : state) {
                count += num;
            }
            keyCountState.update(count);
        }
    }

}
