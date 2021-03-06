package com.spursgdp.flink.streaming.checkpoint;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 滑动窗口计算
 *
 * 通过socket模拟产生单词数据
 * flink对数据进行统计计算
 *
 * 需要实现每隔1秒对最近2秒内的数据进行汇总计算
 *
 * 设置了Checkpoint
 *
 * @author zhangdongwei
 * @create 2020-04-05-10:43
 */
@Slf4j
public class SocketWindowWCWithCheckpoint {

    public static void main(String[] args) throws Exception {

        System.out.println("Java Socket Window Count...");

        //0.获取需要的端口号，如不输入默认给9000
        int port;
        try {
            port = ParameterTool.fromArgs(args).getInt("port");
        } catch (Exception e) {
            port = 9000;
        }

        //1.创建Flink Stream对应的Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置CheckPoint相关信息
        // 每隔1000ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);
        // 高级选项：
        // 设置模式为exactly-once（这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置StateBackend
//       env.setStateBackend(new MemoryStateBackend());
//       env.setStateBackend(new FsStateBackend("hdfs://namenode:9000/flink/checkpoints"));
        StateBackend stateBackend = new RocksDBStateBackend("hdfs://ubuntu:9000/flink/checkpoints", true);
        env.setStateBackend(stateBackend);

        //2.通过env连接socket获取输入的数据源source
        String host = "ubuntu";
//        String host = "192.168.8.117";
        String delimiter = "\n";
        DataStreamSource<String> source = env.socketTextStream(host, port, delimiter);
        //3.进行transform
        DataStream<WordWithCount> windowWordCount = source.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String line, Collector<WordWithCount> collector) throws Exception {
                String[] words = line.split("\\s");
                for (String word : words) {
                    collector.collect(new WordWithCount(word, 1L));
                }
            }
        })
                //基于WordWithCount对象中的word字段进行分组
                .keyBy("word")
                //指定窗口大小，指定间隔时间
                .timeWindow(Time.seconds(5), Time.seconds(1))
                //基于WordWithCount对象中的count字段进行sum聚合
//                .sum("count");
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount w1, WordWithCount w2) throws Exception {
                        WordWithCount wc = new WordWithCount(w1.word, w1.count + w2.count);
                        log.info(wc.toString());
                        return wc;
                    }
                });

        //4.结果输出到控制台，并行度设置为1
        windowWordCount.print().setParallelism(1);

        //5.启动env
        env.execute("Socket Window Count");

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WordWithCount{

        private String word;

        private Long count;

    }
}
