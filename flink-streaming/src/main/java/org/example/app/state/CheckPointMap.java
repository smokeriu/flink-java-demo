package org.example.app.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckPointMap {
    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-java-demo-backend-20210215"));
        //env.setParallelism(3);

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final String hostname = parameterTool.get("hostname");

        final DataStreamSource<String> input2 = env.socketTextStream(hostname, 9999);
        input2.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.valueOf(value);
            }
        }).map(new MyOperatorStateMap())
                .print();

        env.execute();
    }
}
