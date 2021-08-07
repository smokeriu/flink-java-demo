package org.example.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class KafkaUtils {


    /**
     * Basic with No WatermarkStrategy and SimpleStringSchema
     */
    public static DataStream<String> createStreamFromKafka(ParameterTool propsTool,
                                                           String sourceName) {
        return createStreamFromKafka(propsTool, KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()), WatermarkStrategy.noWatermarks(), sourceName);
    }

    public static <T> DataStream<T> createStreamFromKafka(ParameterTool propsTool,
                                                          KafkaRecordDeserializationSchema<T> deserializer,
                                                          String sourceName) {
        return createStreamFromKafka(propsTool, deserializer, WatermarkStrategy.noWatermarks(), sourceName);
    }

    /**
     * 从Kafka创建流
     *
     * @param propsTool         道具的工具
     * @param deserializer      反序列化器
     * @param watermarkStrategy 水印策略
     * @param sourceName        源名称
     * @return {@link DataStream<T>}
     */
    public static <T> DataStream<T> createStreamFromKafka(ParameterTool propsTool,
                                                          KafkaRecordDeserializationSchema<T> deserializer,
                                                          WatermarkStrategy<T> watermarkStrategy,
                                                          String sourceName) {
        // kafka option
        final StreamExecutionEnvironment env = FlinkUtils.getEnv();
        final String bootstrapServers = propsTool.getRequired("bootstrap.servers");
        final String groupId = propsTool.get("group.id", "liushengwei-mac");
        final List<String> topics = Arrays.stream(propsTool.get("source.topics").split(",")).collect(Collectors.toList());

        final KafkaSource<T> kafkaSource = KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setGroupId(groupId)
                .setTopics(topics)
                .setDeserializer(deserializer)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        // checkpoint option
        final String checkpointPath = propsTool.get("checkpoint.path", "file:///Users/liushengwei/IdeaProjects/flink-java-demo/flink-streaming/file/flink-state/" + sourceName);
        final int checkpointInterval = propsTool.getInt("checkpoint.interval",5000 * 60);
        final int minPauseBetweenCheckpoint = propsTool.getInt("checkpoint.pause", 500);
        env.enableCheckpointing(checkpointInterval);
        final CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 最小间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoint);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // todo
//        checkpointConfig.setCheckpointStorage();

        env.setStateBackend(new FsStateBackend(""));


        return env.fromSource(kafkaSource, watermarkStrategy, sourceName);
    }
}
