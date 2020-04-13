package com.spursgdp.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理wordcount
 * @author zhangdongwei
 * @create 2020-04-05-11:21
 */
public class BatchFileWC {

    public static void main(String[] args) throws Exception {
        String inputPath = "D://data/input.data";
        String outputPath = "D://data/output";

        //1.创建Flink Batch对应的Env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.通过env读取离线文件Source
        DataSource<String> source = env.readTextFile(inputPath);

        //3.进行transform
        DataSet<Tuple2<String, Long>> wcDataSet =
            source.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                    String[] words = line.split("\\s");
                    for (String word : words) {
                        collector.collect(new Tuple2<>(word, 1L));
                    }
                }
            })
                .groupBy(0)
                .sum(1);

        //4.结果输出到另一个文件，并行度设置为1，否则结果会输出到多个文件中
//        wcDataSet.collect().forEach(System.out::print);
        wcDataSet.writeAsCsv(outputPath);//.setParallelism(2);

        //5.启动env
        env.execute("Batch File Word Count");
    }


}
