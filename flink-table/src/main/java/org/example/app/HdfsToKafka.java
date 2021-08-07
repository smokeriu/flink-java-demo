package org.example.app;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class HdfsToKafka {

    public static void main(String[] args) {
        Configuration flinkConf = new Configuration();
        flinkConf.setBoolean("recursive.file.enumeration", true);
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment(flinkConf);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);



        String jsonSql = "create table file_tuple (" +
                "sip string," +
                "dip string" +
                ") with (" +
                "'connector' = 'filesystem'," +
                "'path' = 'file:///Users/liushengwei/meta_log/'," +
                "  'format' = 'json'    " +
                ")";
        bsTableEnv.executeSql(jsonSql);

        String printSql = "CREATE TABLE print_table (\n" +
                "sip string," +
                "dip string" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        bsTableEnv.executeSql(printSql);

        final Table input = bsTableEnv.from("file_tuple");

        input.executeInsert("print_table");

    }
}
