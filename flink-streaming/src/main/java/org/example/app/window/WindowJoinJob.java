package org.example.app.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.time.Duration;
import java.util.Properties;

/**
 * Window Join
 * @author liushengwei
 */
public class WindowJoinJob {
    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // kafka props
        final Properties props = new Properties();
        props.put("bootstrap.servers", "cdh02:9092,cdh03:9092,cdh04:9092,cdh05:9092");
        props.setProperty("group.id", "Flink" + System.currentTimeMillis());
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        // set watermark
        final WatermarkStrategy<ObjectNode> watermarkStrategy = WatermarkStrategy.<ObjectNode>forBoundedOutOfOrderness(Duration.ofMillis(1000))
                .withTimestampAssigner(new SerializableTimestampAssigner<ObjectNode>() {
                    @Override
                    public long extractTimestamp(ObjectNode element, long recordTimestamp) {
                        return element.get("value").get("log_time").asLong();
                    }
                });

        final FlinkKafkaConsumer<ObjectNode> consumer1 = new FlinkKafkaConsumer<>("flink_msg_join1", new JSONKeyValueDeserializationSchema(true), props);
        final FlinkKafkaConsumer<ObjectNode> consumer2 = new FlinkKafkaConsumer<>("flink_msg_join2", new JSONKeyValueDeserializationSchema(true), props);


        consumer1.assignTimestampsAndWatermarks(watermarkStrategy);
        consumer2.assignTimestampsAndWatermarks(watermarkStrategy);


        // log_time,key,info1
        final SingleOutputStreamOperator<Tuple3<Long, Integer, String>> input1 = env.addSource(consumer1)
                .map(data -> {
                    final JsonNode recordValue = data.get("value");
                    final long logTime = recordValue.get("log_time").asLong();
                    final int id = recordValue.get("id").asInt();
                    final String info1 = recordValue.get("info1").asText();
                    return Tuple3.of(logTime, id, info1);
                });
        final SingleOutputStreamOperator<Tuple3<Long, Integer, String>> input2 = env.addSource(consumer2)
                .map(data -> {
                    final JsonNode recordValue = data.get("value");
                    final long logTime = recordValue.get("log_time").asLong();
                    final int id = recordValue.get("id").asInt();
                    final String info2 = recordValue.get("info2").asText();
                    return Tuple3.of(logTime, id, info2);
                });

        input1.print();
        input2.print();

        env.execute("window Join");

    }
}
