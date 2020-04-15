package com.spursgdp.flink.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;

/**
 *
 * 基于Blink Batch Table Env + CSV File创建Table
 *
 */
public class TestCreateTableByFile {

    public static void main(String[] args) {
        //1.初始化Blink Batch Table Env
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);
        bbTableEnv.listCatalogs();


        //2.基于csv文件创建表
        String[] fields = {"f1","f2","f3","f4","f5","f6"};
        TypeInformation<?>[] fieldTypes = {Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),Types.LONG(), Types.LONG()};
        CsvTableSource fileSource = new CsvTableSource("/station.log", fields, fieldTypes);
        System.out.println(fileSource.getTableSchema());  //打印File Source的schema

        //3.注册Table，表名为station_log
        bbTableEnv.registerTableSource("station_log", fileSource);

        //4.获取Table，并打印schema
        Table stationTable = bbTableEnv.from("station_log");
        stationTable.printSchema();

        //TODO: 打印Table中的数据？？？

    }


}
