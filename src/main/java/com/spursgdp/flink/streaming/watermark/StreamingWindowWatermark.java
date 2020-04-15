package com.spursgdp.flink.streaming.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.List;

/**
 * Watermark案例，参考教材 P139
 *
 * @author zhangdongwei
 * @create 2020-04-05-10:43
 */
@Slf4j
public class StreamingWindowWatermark {

    public static void main(String[] args) throws Exception {

        System.out.println("Java Socket Window Count...");

        //0.获取需要的端口号，如不输入默认给9000
        int port;
        try {
            port = ParameterTool.fromArgs(args).getInt("port");
        } catch (Exception e) {
            port = 9001;
        }

        //1.创建Flink Stream对应的Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置使用EventTime，默认使用的是ProcessTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置并行度为1，默认是当前机器的CPU数量

        //2.通过env连接socket获取输入的数据源source
        String host = "ubuntu";
        String delimiter = "\n";
        DataStreamSource<String> source = env.socketTextStream(host, port, delimiter);

        //3.解析输入的数据 -> EventWithTime(String,Long,Date)
        SingleOutputStreamOperator<EventWithTime> eventWithTimeStream =
                source.map(new MapFunction<String, EventWithTime>() {
                    @Override
                    public EventWithTime map(String line) throws Exception {
                        String[] words = line.split(",");
                        return new EventWithTime(words[0], Long.valueOf(words[1]));
                    }
                });

        //4.抽取timestamp和watermark，默认100s调用一次
        SingleOutputStreamOperator<EventWithTime> watermarkStream =
                eventWithTimeStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<EventWithTime>() {


                    Long currentMaxTimestamp = 0L;
                    final Long maxOutOfOrderness = 10000L;  //允许最大的乱序时间是10s

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                    /**
                     * 定义如何提取timestamp（即EventTime，因为前面的env指定了要获取EventTime）
                     */
                    @Override
                    public long extractTimestamp(EventWithTime event, long previousElementTimestamp) {
                        //修改currentMaxTimestamp
                        currentMaxTimestamp = Math.max(event.timestamp,currentMaxTimestamp);
                        System.out.println("事件: " + event + " ,此时的currentMaxTimestamp = " + sdf.format(currentMaxTimestamp));
                        return event.timestamp;
                    }

                    /**
                     * 定义生成Watermark的逻辑，默认100ms被调用一次
                     */
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        //当前水印 = 当前最大的timestamp - 允许的最大乱序时间
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }
                });

        //5.设置window，并进行window内的分组聚合
        SingleOutputStreamOperator<String> windowStream = watermarkStream.keyBy("word")
                .timeWindow(Time.seconds(3))  //滚动窗口，3s一个周期，基于EventTime触发
                .process(new ProcessWindowFunction<EventWithTime, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<EventWithTime> inputs, Collector<String> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        List<EventWithTime> events = IteratorUtils.toList(inputs.iterator());
                        events.sort(Comparator.comparingLong(EventWithTime::getTimestamp));
                        StringBuilder result = new StringBuilder();
                        result = result.append("本窗口[" + sdf.format(context.window().getStart()) + " ~ " + sdf.format(context.window().getEnd()) + ")共输出" + events.size() + "条记录：" + System.lineSeparator());
                        for (EventWithTime event : events) {
                            result = result.append(event + System.lineSeparator());
                        }
                        out.collect(result.toString());
                    }

                });

        windowStream.print();

        //5.启动env
        env.execute("Socket Window Count");

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class EventWithTime{

        private String word;

        private Long timestamp;

        private String date;

        public EventWithTime(String word, Long timestamp) {
            this.word = word;
            this.timestamp = timestamp;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            date = sdf.format(timestamp);
        }
    }
}
