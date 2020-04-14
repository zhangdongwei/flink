package com.spursgdp.flink.batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangdongwei
 * @create 2020-04-14-9:32
 */
public class DataSetOuterJoinDemo {

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
        data2.add(new Tuple2<>(4,"guangzhou"));

        //source
        DataSource<Tuple2<Integer, String>> source1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> source2 = env.fromCollection(data2);

        //transformation
        /**
         * 左外连接
         * 注意：second可为null
         */
        System.out.println("==============左外连接================");
        source1.leftOuterJoin(source2)
                //左表第0个字段
                .where(0)
                //右表第0个字段
                .equalTo(0)
                //左表，右表，输出结果
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(second == null){
                            return new Tuple3<Integer,String,String>(first.f0,first.f1,null);
                        }else{
                            return new Tuple3<Integer,String,String>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();
        /**
         * 右外连接
         * 注意：first可为null
         */
        System.out.println("==============右外连接================");
        source1.rightOuterJoin(source2)
                //左表第0个字段
                .where(0)
                //右表第0个字段
                .equalTo(0)
                //左表，右表，输出结果
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(first == null){
                            return new Tuple3<Integer,String,String>(second.f0,null,second.f1);
                        }else{
                            return new Tuple3<Integer,String,String>(second.f0,first.f1,second.f1);
                        }
                    }
                }).print();

        /**
         * 全外连接
         * 注意：first、second都可为null
         */
        System.out.println("==============全外连接================");
        source1.fullOuterJoin(source2)
                //左表第0个字段
                .where(0)
                //右表第0个字段
                .equalTo(0)
                //左表，右表，输出结果
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(first == null){
                            return new Tuple3<Integer,String,String>(second.f0,null,second.f1);
                        }else if(second == null){
                            return new Tuple3<Integer,String,String>(first.f0,first.f1,null);
                        }else{
                            return new Tuple3<Integer,String,String>(second.f0,first.f1,second.f1);
                        }
                    }
                }).print();
    }
}
