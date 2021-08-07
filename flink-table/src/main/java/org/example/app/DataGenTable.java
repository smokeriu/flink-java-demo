package org.example.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SessionWithGap;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.operators.window.assigners.SessionWindowAssigner;

import java.time.Duration;

public class DataGenTable {
    public static void main(String[] args) throws Exception {
        String createSql = "CREATE TABLE Orders (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2),\n" +
                "    order_time   TIMESTAMP(3),\n" +
                "    buyer        String\n"+
                ") WITH (\n" +
                "  'connector' = 'datagen'\n" +
                ")";
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(bsEnv,bsSettings);
        streamTableEnvironment.executeSql(createSql);

        String kafkaSql = "CREATE TABLE order_kafka (\n" +
                "  `order_number` BIGINT,\n" +
                "  `price` DECIMAL(32,2),\n" +
                "  `order_time`   TIMESTAMP(3),\n" +
                "  `buyer`        String\n"+
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'orders_json',\n" +
                "  'properties.bootstrap.servers' = 'PLAINTEXT://localhost:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";

        streamTableEnvironment.executeSql(kafkaSql);

        final Table select = streamTableEnvironment.from("Orders");

        select.executeInsert("order_kafka");

        streamTableEnvironment.toAppendStream(select, String.class)
                .keyBy(elem -> elem.toString())
                .countWindow(1);

    }
}
