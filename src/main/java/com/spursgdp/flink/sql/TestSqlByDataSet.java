package com.spursgdp.flink.sql;

import com.spursgdp.flink.sql.entity.WC;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

/**
 * 基于DataSet创建Table，并输出
 * @author zhangdongwei
 * @create 2020-04-17-9:25
 */
public class TestSqlByDataSet {

    public static void main(String[] args) throws Exception {
        //1.创建Env（Batch Flink Env）
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        //2.创建DataSet
        DataSource<WC> inputDataSet = env.fromElements(
                new WC("Hello", 1),
                new WC("World", 1),
                new WC("Hello", 1)
        );
        //3.注册表（从DataSet）
//        tEnv.registerDataSet("wordcount_table",inputDataSet,"word,frenquence");
        tEnv.registerDataSet("wordcount_table",inputDataSet,"word,frequence");
        //4.执行查询
        Table table = tEnv.sqlQuery("select word, sum(frequence) as frequence from wordcount_table group by word");
        table.printSchema();
        //5.转换并输出
//        DataSet<WC> resultDataSet = tEnv.toDataSet(table, WC.class);
        //6.输出
//        resultDataSet.print();
    }
}
