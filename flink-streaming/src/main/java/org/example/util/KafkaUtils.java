package org.example.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Kafka 相关工具类
 *
 * @author liushengwei
 * @date 2021/08/09
 */
public class KafkaUtils {

    private KafkaUtils() {}

    /**
     * Basic with No WatermarkStrategy and SimpleStringSchema
     */
    public static DataStream<String> createStreamFromKafka(ParameterTool propsTool,
                                                           String sourceName) {
        return createStreamFromKafka(propsTool, KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()), sourceName);
    }

    /**
     * 从Kafka创建流。仅仅包含序列化规则。
     *
     * @param propsTool    道具的工具
     * @param deserializer 反序列化器
     * @param sourceName   源名称
     * @return {@link DataStream<T>}
     */
    public static <T> DataStream<T> createStreamFromKafka(ParameterTool propsTool,
                                                          KafkaRecordDeserializationSchema<T> deserializer,
                                                          String sourceName) {
        return createStreamFromKafka(propsTool, deserializer, WatermarkStrategy.noWatermarks(), sourceName);
    }

    /**
     * 从Kafka创建流, 包含序列化规则以及水印策略
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
        final String bootstrapServers = propsTool.getRequired("source.bootstrap.servers");
        final String groupId = propsTool.get("group.id", "liushengwei-mac");
        final List<String> topics = Arrays.stream(propsTool.getRequired("source.topics").split(",")).collect(Collectors.toList());

        final KafkaSource<T> kafkaSource = KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setGroupId(groupId)
                .setTopics(topics)
                .setDeserializer(deserializer)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        // checkpoint option
        final String checkpointPath = propsTool.get("checkpoint.path", "file:///Users/liushengwei/IdeaProjects/flink-java-demo/flink-streaming/file/flink-state/" + sourceName);
        final int checkpointInterval = propsTool.getInt("checkpoint.interval", 1000 * 60);
        final int minPauseBetweenCheckpoint = propsTool.getInt("checkpoint.pause", 500);
        final int restartAttempts = propsTool.getInt("restart.attempt.number", 5);
        final int restartAttemptInterval = propsTool.getInt("restart.attempts.interval", 5);

        // enableCheckpoint
        env.enableCheckpointing(checkpointInterval);
        final CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 最小间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoint);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // StateBackend
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage(checkpointPath));
        // RestartStrategy
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, Time.seconds(restartAttemptInterval)));

        return env.fromSource(kafkaSource, watermarkStrategy, sourceName);
    }


    /**
     * 创建String 类型的简单的Kafka Producer
     *
     * @param propsTool    配置
     * @param outputStream 输出流
     * @return {@link DataStreamSink}<{@link String}>
     */
    public static DataStreamSink<String> createKafkaProducer(ParameterTool propsTool,DataStream<String> outputStream){
        return createKafkaProducer(propsTool,new SimpleStringSchema(),outputStream);
    }

    /**
     * 创建Kafka Producer
     *
     * @param propsTool           配置
     * @param kafkaSerializationSchema 串行化模式
     * @param outputStream        输出流
     * @return {@link DataStreamSink}<{@link T}>
     */
    public static <T> DataStreamSink<T> createKafkaProducer(ParameterTool propsTool,
                                                            KafkaSerializationSchema<T> kafkaSerializationSchema,
                                                            DataStream<T> outputStream){
        // kafka config
        final String sinkTopic = propsTool.getRequired("sink.topic");
        final String sinkBootstrapServers = propsTool.getRequired("sink.bootstrap.servers");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", sinkBootstrapServers);
        FlinkKafkaProducer<T> myProducer = new FlinkKafkaProducer<>(
                sinkTopic,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        return outputStream.addSink(myProducer);
    }

    /**
     * 创建Kafka Producer
     *
     * @param propsTool           配置
     * @param serializationSchema 串行化模式
     * @param outputStream        输出流
     * @return {@link DataStreamSink}<{@link T}>
     */
    public static <T> DataStreamSink<T> createKafkaProducer(ParameterTool propsTool,
                                                              SerializationSchema<T> serializationSchema,
                                                            DataStream<T> outputStream){

        final String sinkTopic = propsTool.getRequired("sink.topic");
        final KafkaSerializationSchemaWrapper<T> tKafkaSerializationSchemaWrapper =
                new KafkaSerializationSchemaWrapper<>(sinkTopic, new FlinkFixedPartitioner<>(), false, serializationSchema);
        return createKafkaProducer(propsTool, tKafkaSerializationSchemaWrapper, outputStream);
    }
}
