package com.spursgdp.flink.batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangdongwei
 * @create 2020-04-14-9:18
 */
public class DataSetJoinDemo {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //tuple2<用户id，用户姓名>
        List<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(2,"ls"));
        data1.add(new Tuple2<>(3,"ww"));


        //tuple2<用户id，用户所在城市>
        List<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"beijing"));
        data2.add(new Tuple2<>(2,"shanghai"));
        data2.add(new Tuple2<>(3,"guangzhou"));

        //source
        DataSource<Tuple2<Integer, String>> source1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> source2 = env.fromCollection(data2);

        //transformation
        //法1：with
        source1.join(source2)
                //左表第0个字段
                .where(0)
                //右表第0个字段
                .equalTo(0)
                //参数1，参数2，输出结果
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
                    }
                }).print();

        System.out.println("===============================================================");

        //法2：map
        source1.join(source2)
                //左表第0个字段
                .where(0)
                //右表第0个字段
                .equalTo(0)
                //(参数1，参数2），输出结果
                .map(new MapFunction<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> t) throws Exception {
                        return new Tuple3<Integer, String, String>(t.f0.f0,t.f0.f1,t.f1.f1);
                    }
                }).print();

    }

}
