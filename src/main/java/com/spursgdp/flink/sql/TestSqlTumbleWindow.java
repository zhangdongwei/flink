package com.spursgdp.flink.sql;

import com.spursgdp.flink.sql.entity.StationLog;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Flink SQL + Tumble Window （需要Event Time + Watermark）
 * 案例：每3秒钟统计一次每个基站的通话成功时间总和。
 */
public class TestSqlTumbleWindow {

    public static void main(String[] args) throws Exception {
        //1.创建Blink Stream Env
        EnvironmentSettings bsSettings  = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        //2.指定EventTime为时间语义
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        bsEnv.setParallelism(1);

        //3.source: 接收流数据
        DataStreamSource<String> streamSource = bsEnv.socketTextStream("ubuntu", 9001, "\n");

        //4.transform：转变为StationgLog对象
        SingleOutputStreamOperator<StationLog> stationStream = streamSource.map(new MapFunction<String, StationLog>() {
            @Override
            public StationLog map(String line) throws Exception {
                String[] arr = line.split(",");
                StationLog stationLog = new StationLog(arr[0].trim(), arr[1].trim(), arr[2].trim(), arr[3].trim(), Long.valueOf(arr[4].trim()), Long.valueOf(arr[5].trim()));
                return stationLog;
            }
        })
                //引入watermark，可以接收2s的数据乱序
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<StationLog>(Time.seconds(2)) {
                    //指定EventTime对应的字段
                    @Override
                    public long extractTimestamp(StationLog stationLog) {
                        return stationLog.getCallTime();
                    }
                });

        //5. 将DataStream注册到TableEnv中，并设置时间属性
        bsTableEnv.registerDataStream("station_log",stationStream, "sid,callInt,callOut,callType,duration,callTime.rowtime");
        Table resultTable = bsTableEnv.sqlQuery(
                "select sid, " +
                        "max(s.callType), " +
                        "count(*), " +
                        "sum(duration), " +
                        "tumble_start(s.callTime,interval '3' second) as windowStart," +
                        "tumble_end(s.callTime,interval '3' second) as windowEnd " +
                "from station_log s " +
                "where s.callType='success' " +
                "group by tumble(s.callTime,interval '3' second),sid"
        );

        resultTable.printSchema();
        DataStream<Tuple2<Boolean, Row>> resultDs = bsTableEnv.toRetractStream(resultTable, Row.class);
        resultDs.filter((FilterFunction<Tuple2<Boolean, Row>>) t -> t.f0 == true).print();

        bsEnv.execute("TestSqlTumbleWindow");
    }
}
