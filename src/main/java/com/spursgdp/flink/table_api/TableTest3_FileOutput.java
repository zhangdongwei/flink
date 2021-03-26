package com.spursgdp.flink.table_api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author zhangdongwei
 * @create 2021-03-24-15:42
 */
public class TableTest3_FileOutput {

    public static void main(String[] args) throws Exception {

        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


//        // 1.1 基于老版本planner的流处理
//        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
//                .useOldPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);
//
//        // 1.2 基于老版本planner的批处理
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);
//
//        // 1.3 基于Blink的流处理
//        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);
//
//        // 1.4 基于Blink的批处理
//        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inBatchMode()
//                .build();
//        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);

        //2. 创建表（文件）
        String filepath = "D:\\projects\\flink\\src\\main\\resources\\sensor.txt";
        tableEnv.connect(new FileSystem().path(filepath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                    .field("timestamp", DataTypes.BIGINT())
                    .field("temprature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        //2.1 创建Table对象
        Table inputTable = tableEnv.from("inputTable");

        //3.Table API
        //简单转换
        Table resultTable = inputTable.select("id,temprature AS temp").where("id='sensor_1'");
        //聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id, id.count AS count, temprature.avg AS avgTemp");

        //4.SQL
        //简单转换
        Table sqlResultTable = tableEnv.sqlQuery("select id, temprature AS temp from inputTable where id = 'sensor_1'");
        //聚合统计
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) AS cnt, avg(temprature) AS avtTemp from inputTable group by id");
//        sqlAggTable.printSchema();
//        tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlAggTable");

        //5.输出到文件
        String outputPath = "D:\\projects\\flink\\src\\main\\resources\\out.txt";
        tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");
        sqlResultTable.executeInsert("outputTable");

        env.execute();

    }
}
