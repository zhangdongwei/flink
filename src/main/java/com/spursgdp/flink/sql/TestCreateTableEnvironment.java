package com.spursgdp.flink.sql;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 创建4种TableEnvironment：
 *     Flink Stream Env
 *     Flink Batch Env
 *     Blink Stream Env
 *     Blink Batch Env
 */
public class TestCreateTableEnvironment {

    public static void main(String[] args) {

        // ***************************
        // FLINK STREAMING QUERY(OLD)
        // ***************************
        EnvironmentSettings fsSettings  = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
        System.out.println(fsTableEnv);

        // ***********************
        // FLINK BATCH QUERY(OLD)
        // ***********************
        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTabelEnv = BatchTableEnvironment.create(fbEnv);
        System.out.println(fbTabelEnv);

        // *********************
        // BLINK STREAMING QUERY
        // *********************
        EnvironmentSettings bsSettings  = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        System.out.println(bsTableEnv);

        // ******************
        // BLINK BATCH QUERY
        // ******************
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);
        System.out.println(bbTableEnv);

    }

}
