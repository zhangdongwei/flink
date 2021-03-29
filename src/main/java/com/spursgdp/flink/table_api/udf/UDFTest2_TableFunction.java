package com.spursgdp.flink.table_api.udf;

import com.spursgdp.flink.streaming.state.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author zhangdongwei
 * @create 2021-03-29-14:33
 */
public class UDFTest2_TableFunction {

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


        // 4.在tableEnv中注册SplitExplode UDF，实现将id拆分，并输出（word, length）
        SplitExplode splitExplode = new SplitExplode("_");
        tableEnv.registerFunction("split_explode", splitExplode);

        // 5.SQL
        tableEnv.createTemporaryView("sensor", dataTable);
        String sql = "select id, word, length from sensor, lateral table(split_explode(id)) AS splitId(word, length)";
        Table sqlResultTable = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(sqlResultTable, Row.class).print("sqlResultTable");

        env.execute();
    }

    /**
     * 自定义UDF类 -> Table Function
     */
    public static class SplitExplode extends TableFunction<Tuple2<String, Integer>> {

        private String separator = ",";

        public SplitExplode(String separator) {
            this.separator = separator;
        }

        // 必须实现一个eval方法，没有返回值
        public void eval(String field) {
            String[] splits = field.split(separator);
            for (String str : splits) {
                Tuple2<String, Integer> t = new Tuple2<>(str, str.length());
                collect(t);
            }
        }
    }
}
