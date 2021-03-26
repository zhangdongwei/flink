package com.spursgdp.flink.table_api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author zhangdongwei
 * @create 2021-03-24-15:42
 */
public class TableTest4_KafkaPipeline {

    public static void main(String[] args) throws Exception {

        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 连接Kafka，读取数据，创建表
        tableEnv.connect(new Kafka()
                            .version("0.11")
                            .topic("sensor")
                            .property("zookeeper.connect", "hnode2:2181")
                            .property("bootstrap.servers", "hnode15:9092")
                         )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temprature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        //2.1 创建Table对象
        Table inputTable = tableEnv.from("inputTable");

        //3. SQL
        Table sqlResultTable = tableEnv.sqlQuery("select id, temprature from inputTable where id = 'sensor_1'");
        sqlResultTable.printSchema();
        tableEnv.toAppendStream(sqlResultTable, Row.class).print("sqlResultTable");

        // 4. 连接Kafka，写入数据，创建表
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor1")
                .property("zookeeper.connect", "hnode2:2181")
                .property("bootstrap.servers", "hnode15:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
//                        .field("timestamp", DataTypes.BIGINT())
                        .field("temprature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");
        sqlResultTable.executeInsert("outputTable");

        env.execute();

    }
}
