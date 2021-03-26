package com.spursgdp.flink.table_api;

import com.spursgdp.flink.streaming.state.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zhangdongwei
 * @create 2021-03-24-14:33
 */
public class TableTest5_TimeAndWindow {

    public static void main(String[] args) throws Exception {

        // 0.创建env、tableEnv
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\projects\\flink\\src\\main\\resources\\sensor.txt");

        // 2.转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading s) {
                return s.getTimestamp();
            }
        });

        // 3.流 -> 表
        Table dataTable = tableEnv.fromDataStream(dataStream, "id, temprature AS temp, timestamp, rt.rowtime");
        tableEnv.createTemporaryView("sensor", dataTable);

        // 4. Flink SQL + 窗口操作（统计过去10s的id分组聚合结果）
        String sql = "select id, count(id) as cnt, avg(temp) as avgTemp, tumble_end(rt, interval '10' second) " +
                     "from sensor group by id, TUMBLE(rt, interval '10' second)";      //滚动窗口（窗口长度=10s）
        Table sqlResultTable = tableEnv.sqlQuery(sql);
        sqlResultTable.printSchema();
        tableEnv.toAppendStream(sqlResultTable, Row.class).print("sqlResultTable");

        env.execute();
    }
}
