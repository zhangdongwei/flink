package com.spursgdp.flink.streaming.state;

import org.apache.flink.api.common.functions.MapFunction;
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
public class StateTest1_OperatorState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 文件输入流
        DataStream<String> inputStream = env.readTextFile("D:\\projects\\flink\\src\\main\\resources\\sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream.map(new MyMapFunction());

        dataStream.print("dataStream");
        resultStream.print("resultStream");

        env.execute();
    }

    public static class MyMapFunction implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {

        //定义一个本地变量，作为算子状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }

        /**
         * 快照：持久化Checkpoint，具体持久化到哪，取决于所用的状态后端StateBackend
         */
        @Override
        public List snapshotState(long checkpointId, long timestamp) throws Exception {
            //这里必须通过list存储状态数据，因为恢复的时候可能会一个分区变成多个分区
            return Collections.singletonList(count);
        }

        /**
         * 恢复：恢复Checkpoint：通过持久化后的数据恢复
         */
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num : state) {
                count += num;
            }
        }

    }

}
