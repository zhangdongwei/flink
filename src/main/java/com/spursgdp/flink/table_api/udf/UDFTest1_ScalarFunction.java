package com.spursgdp.flink.table_api.udf;

import com.spursgdp.flink.streaming.state.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @author zhangdongwei
 * @create 2021-03-29-14:33
 */
public class UDFTest1_ScalarFunction {

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

        // 4.在tableEnv中注册HashCode UDF
        HashCode hashCode = new HashCode(5);
        tableEnv.registerFunction("hashCode", hashCode);

        // 5.SQL：求id的hash值
        tableEnv.createTemporaryView("sensor", dataTable);
        String sql = "select id, hashCode(id), temprature from sensor";
        Table sqlResultTable = tableEnv.sqlQuery(sql);

        // 6.Table -> 流，打印
        tableEnv.toAppendStream(sqlResultTable, Row.class).print("sqlResultTable");

        env.execute();
    }

    /**
     * 自定义UDF类 -> Scalar Function
     */
    public static class HashCode extends ScalarFunction {

        private int factor = 13;

        public HashCode(int factor) {
            this.factor = factor;
        }

        public int eval(String str) {
            return str.hashCode() * factor;
        }
    }
}
