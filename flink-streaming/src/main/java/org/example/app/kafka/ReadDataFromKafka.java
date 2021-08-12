package org.example.app.kafka;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.example.util.FlinkUtils;
import org.example.util.KafkaUtils;

public class ReadDataFromKafka {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        final KafkaRecordDeserializationSchema<ObjectNode> jsonSchemaWithMeta = KafkaRecordDeserializationSchema.of(new JSONKeyValueDeserializationSchema(true));

        final DataStream<ObjectNode> streamFromKafka = KafkaUtils.createStreamFromKafka(parameterTool, jsonSchemaWithMeta,"hadoop-kafka");

        streamFromKafka.print();


        FlinkUtils.getEnv().execute();
    }
}
