package com.spursgdp.flink.batch;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

import java.io.File;

/**
 * @author zhangdongwei
 * @create 2020-04-14-11:04
 */
public class BatchDistributedCacheDemo {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String cacheFileName = "input.data";

        //1：注册一个文件,可以使用hdfs或者s3上的文件，flink启动的时候会将该文件缓存到本地
        env.registerCachedFile("d:\\data\\input.data",cacheFileName);
//        env.registerCachedFile("hdfs://10.13.1.32:9000/zdw/input.data",cacheFileName);

        //source
        DataSource<String> source = env.fromElements("a", "b", "c", "d");

        //transform
        MapOperator<String, String> resultDS = source.map(new RichMapFunction<String, String>() {

            private String fileContent = "";

            @Override
            public void open(Configuration parameters) throws Exception {
                File cacheFile = this.getRuntimeContext().getDistributedCache().getFile(cacheFileName);
                fileContent = FileUtils.readFileToString(cacheFile, "utf8");
            }

            @Override
            public String map(String value) throws Exception {
                return value + "~~~~~~~~~~~~~~~~~~~~~~" + fileContent;
            }
        });

        resultDS.print();

    }


}
