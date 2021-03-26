package com.spursgdp.flink.sql;

import com.spursgdp.flink.sql.entity.StationLog;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 基于Blink Stream Table Env + DataStream创建Table，并执行sql
 */
public class TestCreateTableByDataStream {

    public static void main(String[] args) throws Exception {

        //1.创建Blink Stream Env
        EnvironmentSettings bsSettings  = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        System.out.println(bsTableEnv);

        //2.source: 接收流数据
        DataStreamSource<String> streamSource = bsEnv.socketTextStream("ubuntu", 9001, "\n");

        //3.transform：转变为StationgLog对象
        SingleOutputStreamOperator<StationLog> stationStream = streamSource.map(new MapFunction<String, StationLog>() {
            @Override
            public StationLog map(String line) throws Exception {
                String[] arr = line.split(",");
                StationLog stationLog = new StationLog(arr[0].trim(), arr[1].trim(), arr[2].trim(), arr[3].trim(), Long.valueOf(arr[4].trim()), Long.valueOf(arr[5].trim()));
                return stationLog;
            }
        });

        //4. 将DataStream -> Table
        Table stationTable = bsTableEnv.fromDataStream(stationStream);
        stationTable.printSchema();
        //输出Table的值，需要先转换为DataStream
//        DataStream<StationLog> ds = bsTableEnv.toAppendStream(stationTable, StationLog.class);
//        ds.print();

        //5.使用Table API，进行where、select、groupby
//        Table resultTable1 = stationTable.filter("callType === 'success'");
//        DataStream<StationLog> ds1 = bsTableEnv.toAppendStream(resultTable1, StationLog.class);
//        ds1.print();
//        Table resultTable2 = stationTable.filter("callType === 'success'")
//                                        .groupBy("sid")
//                                        .select("sid, sid.count AS logCount, duration.sum AS totalDuration");
//        DataStream<Tuple2<Boolean, Row>> ds2 = bsTableEnv.toRetractStream(resultTable2, Row.class);
//        ds2.filter((FilterFunction<Tuple2<Boolean, Row>>) t -> t.f0 == true).print(); //状态中新增的数据加了一个false标签

        //6. 将DataStream注册成table表
        bsTableEnv.registerDataStream("station_log",stationStream);
//        Table sqlTable = bsTableEnv.sqlQuery("select * from station_log s where s.callType = 'success'");
//        DataStream<StationLog> ds3 = bsTableEnv.toAppendStream(sqlTable, StationLog.class);
//        ds3.print();
        Table sqlTable = bsTableEnv.sqlQuery("select s.sid,count(*) as c from station_log s where s.callType = 'success' group by s.sid ");
        DataStream<Tuple2<Boolean, Row>> ds3 = bsTableEnv.toRetractStream(sqlTable, Row.class);
        ds3.filter((FilterFunction<Tuple2<Boolean, Row>>) t -> t.f0 == true).print();  //状态中新增的数据加了一个false标签

        bsEnv.execute("TestCreateTableByDataStream");

    }

}
