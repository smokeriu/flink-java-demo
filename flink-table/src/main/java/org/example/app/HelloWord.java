package org.example.app;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author liushengwei
 */
public class HelloWord {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        final TableEnvironment tEnv = TableEnvironment.create(bsSettings);

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
        bsTableEnv.executeSql(kafkaSql);
        String printSql = "CREATE TABLE print_table (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2),\n" +
                "    order_time   TIMESTAMP(3),\n" +
                "    buyer        String\n"+
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";
        bsTableEnv.executeSql(printSql);

        bsTableEnv.from("order_kafka").executeInsert("print_table");

        final DataStreamSource<String> stringDataStreamSource = bsEnv.readTextFile("textPath");

    }
}
