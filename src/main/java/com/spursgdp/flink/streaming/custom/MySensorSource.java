package com.spursgdp.flink.streaming.custom;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 自定义Source类，用来周期性的随机产生Sensor对象
 * @author zhangdongwei
 * @create 2020-12-29-14:06
 */
public class MySensorSource implements SourceFunction<Sensor> {

    private boolean running = true;

    Random r = new Random();

    @Override
    public void run(SourceContext<Sensor> ctx) throws Exception {

        while(running) {
            //随机生成[1-10]的id
            int id = r.nextInt(10) + 1;
            //随机生成符合高斯分布的temprature
            double temprature = 60 + r.nextGaussian() * 20;
            Sensor sensor = new Sensor("sensor_" + id, System.currentTimeMillis(), temprature);
            ctx.collect(sensor);

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}


