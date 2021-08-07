package org.example.app.kafka;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.util.FlinkUtils;
import org.example.util.KafkaUtils;

public class ReadDataFromKafka {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        final DataStream<String> streamFromKafka = KafkaUtils.createStreamFromKafka(parameterTool, "hadoop-kafka");
        streamFromKafka.print();

        FlinkUtils.getEnv().execute();
    }
}
