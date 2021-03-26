package com.spursgdp.flink.table_api;

import com.spursgdp.flink.streaming.state.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zhangdongwei
 * @create 2021-03-24-14:33
 */
public class TableTest1_Example {

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
        Table dataTable = tableEnv.fromDataStream(dataStream);  //dataStream中存储的是POJO类

        // 4.调用table API进行转换操作
        Table resultTable = dataTable.select("id,temprature").where("id='sensor_1'");

        // 5.执行SQL
        tableEnv.createTemporaryView("sensor", dataTable);
        Table sqlResultTable = tableEnv.sqlQuery("select id, count(id) AS cnt, avg(temprature) AS avgTemp from sensor group by id");

        // 6.Table -> 流，打印
//        tableEnv.toAppendStream(dataTable, Row.class).print("dataTable");
//        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
        tableEnv.toRetractStream(sqlResultTable, Row.class).print("sqlResultTable");


        env.execute();
    }
}
