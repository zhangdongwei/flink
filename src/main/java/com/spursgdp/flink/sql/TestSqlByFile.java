package com.spursgdp.flink.sql;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

/**
 *
 * 基于Blink Batch Table Env + CSV File创建Table，然后给予SQL进行查询处理
 *
 * 案例： 统计每个基站通话成功的通话时长总和。
 *
 */
public class TestSqlByFile {

    public static void main(String[] args) throws Exception {
        //1.初始化Blink Stream Table Env
        EnvironmentSettings bsSettings  = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        //2.基于csv文件创建表
        String[] fields = {"sid","callOut","callInt","callType","callTime","duration"};
        TypeInformation<?>[] fieldTypes = {Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),Types.LONG(), Types.LONG()};
//        CsvTableSource fileSource = new CsvTableSource("/station.log", fields, fieldTypes);
        //由于使用的是StreamEnv，所以必须要写绝对路径，如果用BatchEnv，可以使用相对路径
        CsvTableSource fileSource = new CsvTableSource("D:\\projects\\flink\\src\\main\\resources\\station.log", fields, fieldTypes);
        System.out.println(fileSource.getTableSchema());  //打印File Source的schema

        //3.注册Table，表名为station_log
        bsTableEnv.registerTableSource("station_log", fileSource);

        //4.sql查询
        Table resultTable = bsTableEnv.sqlQuery(
                "select s.sid, max(s.callType), sum(s.duration) as sumDuration " +
                "from station_log s " +
                "where s.callType = 'success' " +
                "group by s.sid"
        );
        DataStream<Tuple2<Boolean, Row>> ds = bsTableEnv.toRetractStream(resultTable, Row.class);
        ds.filter((FilterFunction<Tuple2<Boolean, Row>>) t -> t.f0 == true).print();  //状态中新增的数据加了一个false标签

        bsEnv.execute("TestSqlByFile");

    }
}
