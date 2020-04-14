package com.spursgdp.flink.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * broadcast广播变量
 *
 * 需求：
 *     flink会从数据源中获取到用户的姓名，最终需要把用户的姓名和年龄信息打印出来
 *
 * 分析：
 *    需要在中间的map处理的时候获取用户的年龄信息
 *    建议把用户的关系数据集使用广播变量进行处理
 *
 * 注意：
 *    如果多个算子需要使用同一份数据集，那么需要在对应的多个算子后面分别注册广播变量
 *
 */

public class BatchBroadcastDemo {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1：准备需要广播的数据，也是一个DataSet
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("zs",18));
        broadData.add(new Tuple2<>("ls",20));
        broadData.add(new Tuple2<>("ww",17));
        DataSource<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);

        //1.1:处理需要广播的数据,把数据集转换成map<String,Integer>类型，map中的key就是用户姓名，value就是用户年龄
        DataSet<Map<String, Integer>> broadDataSet = tupleData.map(new MapFunction<Tuple2<String, Integer>, Map<String, Integer>>() {
            @Override
            public Map<String, Integer> map(Tuple2<String, Integer> t) throws Exception {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(t.f0, t.f1);
                return map;
            }
        });

        //source
        DataSource<String> source = env.fromElements("zs", "ls", "ww");

        //transformation
        //注意：在这里需要使用到RichMapFunction获取广播变量（而不是普通的MapFunction）
        DataSet<Tuple2<String, Integer>> resultDataSet = source.map(new RichMapFunction<String, Tuple2<String, Integer>>() {

            Map<String, Integer> broadcastMap = new HashMap<String, Integer>();

            /**
             * 这个方法只会执行一次
             * 可以在这里实现一些初始化的功能
             *
             * 所以，可以在open方法中获取广播变量数据
             *
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //3.获取广播变量数据
                List<HashMap<String, Integer>> broadcastMapName = getRuntimeContext().getBroadcastVariable("broadcastMapName");
                for (HashMap<String, Integer> map : broadcastMapName) {
                    broadcastMap.putAll(map);
                }
            }

            @Override
            public Tuple2<String, Integer> map(String name) throws Exception {
                Integer age = broadcastMap.get(name);
                return new Tuple2<String, Integer>(name, age);
            }
        })
        //2：执行广播数据的操作，将broadDataSet以广播数据的形式广播道上面的task中
        .withBroadcastSet(broadDataSet, "broadcastMapName");

        //sink
        resultDataSet.print();

    }


}
