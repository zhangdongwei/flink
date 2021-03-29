package com.spursgdp.flink.table_api.udf;

import com.spursgdp.flink.streaming.state.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @author zhangdongwei
 * @create 2021-03-29-14:33
 */
public class UDFTest3_AggregateFunction {

    public static void main(String[] args) throws Exception {

        // 0.创建env、tableEnv
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\projects\\flink\\src\\main\\resources\\sensor.txt");

        // 2.转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3.流 -> 表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        // 4.在tableEnv中注册AvgTempFunction UDF，求平均值
        AvgTempFunction avgTempFunction = new AvgTempFunction();
        tableEnv.registerFunction("avg_temp", avgTempFunction);

        // 5.SQL：按照id求温度的平均值
        tableEnv.createTemporaryView("sensor", dataTable);
        String sql = "select id, avg_temp(temprature) from sensor group by id";
        Table sqlResultTable = tableEnv.sqlQuery(sql);
        tableEnv.toRetractStream(sqlResultTable, Row.class).print("sqlResultTable");

        env.execute();
    }

    /**
     * 自定义UDF类 -> Aggregate Function
     */
    public static class AvgTempFunction extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<Double, Integer>(0.0, 0);
        }

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        // 必须实现一个accumulate方法，来数据之后更新状态
        public void accumulate(Tuple2<Double, Integer> accumulator, Double temprature) {
            accumulator.f0 += temprature;
            accumulator.f1 += 1;
        }

    }

}
