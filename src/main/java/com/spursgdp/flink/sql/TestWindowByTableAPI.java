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
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 案例：统计最近 5 秒钟，每个基站的呼叫数量，允许数据的最大乱序时间是2s（需要EventTime + Watermark）
 */
public class TestWindowByTableAPI {

    public static void main(String[] args) throws Exception {

        //1.创建Blink Stream Env
        EnvironmentSettings bsSettings  = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        //2.指定EventTime为时间语义
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        bsEnv.setParallelism(1);

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

        //5. 将DataStream -> Table，并设置时间属性
//        Table stationTable = bsTableEnv.fromDataStream(stationStream);
        Table stationTable = bsTableEnv.fromDataStream(stationStream, "sid,callInt,callOut,callType,duration,callTime.rowtime");
        stationTable.printSchema();

        //6. 滚动窗口（5s）
        GroupWindowedTable windowTable = stationTable.window(Tumble.over("5.second").on("callTime").as("window"));
        //   滑动窗口（5s、2s）
//        GroupWindowedTable windowTable = stationTable.window(Slide.over("5.second").every("2.second").on("callTime").as("window"));
        Table resultTable = windowTable.groupBy("window,sid")  //必须使用两个字段分组，分别是窗口和基站ID
                                       .select("sid, window.start, window.end, window.rowtime, sid.count"); //聚合计算
        resultTable.printSchema();
        DataStream<Tuple2<Boolean, Row>> resultDs = bsTableEnv.toRetractStream(resultTable, Row.class);
        resultDs.filter((FilterFunction<Tuple2<Boolean, Row>>) t -> t.f0 == true).print();

        bsEnv.execute("TestWindowByTableAPI");
    }

}
