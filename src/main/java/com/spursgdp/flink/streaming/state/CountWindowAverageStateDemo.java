package com.spursgdp.flink.streaming.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhangdongwei
 * @create 2020-04-14-11:28
 */
public class CountWindowAverageStateDemo {

    public static void main(String[] args) throws Exception {

        //1.创建Flink对应的Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.source
        DataStreamSource<Tuple2<Long, Long>> source = env.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(1L, 5L),
                Tuple2.of(1L, 7L),
                Tuple2.of(1L, 4L),
                Tuple2.of(1L, 2L),
                Tuple2.of(1L, 9L),
                Tuple2.of(1L, 4L)
        );

        //3.transform
        SingleOutputStreamOperator<Tuple2<Long, Long>> resultSink = source.keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {

                    /**
                     * ValueState状态句柄，第一个值为count，第二个值为sum
                     */
                    private transient ValueState<Tuple2<Long, Long>> sum;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //1.初始化状态sum
                        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                                new ValueStateDescriptor<>(
                                        "average",   //状态名称
                                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                                        }), //状态类型
                                        Tuple2.of(0L, 0L)  //状态的初始值
                                );
                        sum = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
                        //2.获取当前sum的状态值
                        Tuple2<Long, Long> currentSum = sum.value();
                        //3.更新状态值
                        currentSum.f0 += 1;
                        currentSum.f1 += input.f1;
                        sum.update(currentSum);
                        //4.如果count >= 3，清空状态值，重新计算
                        if (currentSum.f0 >= 2) {
                            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
                            sum.clear();
                        }

                    }
                });

        //4. sink
        resultSink.print();

        //执行
        env.execute("CountWindowAverageStateDemo");
    }



}
