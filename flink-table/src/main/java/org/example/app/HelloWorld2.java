package org.example.app;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class HelloWorld2 {
    public static void main(String[] args) {
        final EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(environmentSettings);
        String createSql = "CREATE TABLE table_test (\n" +
                "  id int,\n" +
                "  name STRING,\n" +
                "  info string\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/flink_test',\n" +
                "   'username' = 'root',\n"+
                "   'password' = 'root',\n"+
                "   'table-name' = 'table_test'\n" +
                ")";
        tableEnv.executeSql(createSql);
        final TableResult sqlResult = tableEnv.executeSql("select id , name , info from table_test");
        sqlResult.print();

    }
}
