package com.spursgdp.flink.streaming.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Keyed State编程示例：检测传感器的温度值，如果连续的两个温度差值超过 10 度，就输出报警。
 * @author zhangdongwei
 * @create 2021-01-05-10:01
 */
public class StateTest3_KeyedStateApplicationCase {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        //2.检查点配置
//        env.enableCheckpointing(300);
//
//        // 高级选项
//        //一致性级别
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //超时写入时间（1分钟）
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        //同时进行保存checkpoint的数量
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        //两次Checkpoint保存之间的时间间隔不能小于100ms，因为要留出一段时间处理数据，该配置可以覆盖上面的配置
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
//        //允许当前Checkpoint保存失败1次（默认是0，如果Checkpoint挂了则任务也就挂了）
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);
//
//        // 3.重启策略
//        // 固定延迟重启（每隔10s重启一次，3次重启失败就报错）
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L));
//        // 失败率重启
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));

        // Socket输入流
        DataStream<String> inputStream =  env.socketTextStream("localnode2", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前key对应的数据个数，所以必须要先进行keyBy操作后才能使用KeyedState
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy("sensorId").flatMap(new TempIncreaseWarning(10.0));

        dataStream.print("dataStream");
        resultStream.print("resultStream");

        env.execute();
    }

    public static class TempIncreaseWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        private Double threshold = 0.0;

        public TempIncreaseWarning(Double threshold) {
            this.threshold = threshold;
        }

        //状态变量：保存上一次的温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double currentTemp = sensorReading.getTemprature();
            Double lastTemp = lastTempState.value();
            if(lastTemp != null) {
                if(Math.abs(currentTemp - lastTemp) >= threshold) {
                    out.collect(new Tuple3<String, Double, Double>(sensorReading.getId(), lastTemp, currentTemp));
                }
            }
            //更新state
            lastTempState.update(currentTemp);
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }

}
