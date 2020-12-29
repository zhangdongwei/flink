package com.spursgdp.flink.streaming.asyncIO;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;



/**
 * 异步IO
 */
public class AsyncIODemo {

    public static void main(String[] args) throws Exception {

        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        //source
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //transform
        SingleOutputStreamOperator<String> stream1 =
//                AsyncDataStream.orderedWait(
                AsyncDataStream.unorderedWait(
                        source,
                        new MyAsyncFunction(),
                        1,     //查询超时：请求的最长等待时间
                        TimeUnit.MINUTES,
                        20  //并发线程数：同一时间异步请求并发数
                );

        //sink
        stream1.print().setParallelism(1);

        //execute
        env.execute("AsyncIODemo");


    }

    static class MyAsyncFunction extends RichAsyncFunction<Integer, String> {

        //线程池
        ExecutorService executors;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            executors = Executors.newFixedThreadPool(4);   //初始化线程池
        }

        @Override
        public void close() throws Exception {
            super.close();
            executors.shutdown();   //关闭线程池
        }

        @Override
        public void asyncInvoke(Integer elem, ResultFuture<String> outputs) {
            CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(new Supplier<String>() {
                @Override
                public String get() {
                    try {
                        System.out.println(Thread.currentThread().getName() + " run...");
                        Thread.sleep(2000L);
                        return elem + "..." + Thread.currentThread().getName();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            },executors);
            completableFuture.thenAccept(str -> outputs.complete(Collections.singletonList(str)));
        }
    }

}
