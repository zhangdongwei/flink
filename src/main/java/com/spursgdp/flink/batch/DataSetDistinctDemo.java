package com.spursgdp.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * @author zhangdongwei
 * @create 2020-04-06-11:29
 */
public class DataSetDistinctDemo {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //source
        List<String> data = Arrays.asList("hello spark", "hello flink", "hello hadoop", "hello hadoop flink");

        DataSource<String> source = env.fromCollection(data);

        //transform
        DataSet<String> flatMapData = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.split("\\W+");
                for (String word : words) {
                    System.out.println("单词：" + word);
                    out.collect(word);
                }
            }
        });

        System.out.println("===========================================");
        flatMapData.distinct().print();

        //Batch处理可以不加这句
//        env.execute("DataSet Distinct Demo...");

    }
}

