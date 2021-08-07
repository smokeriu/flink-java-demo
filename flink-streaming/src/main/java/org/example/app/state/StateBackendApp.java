package org.example.app.state;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class StateBackendApp {
    public static void main(String[] args) throws IOException {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final String backendPath = parameterTool.get("backendPath");
        final RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(backendPath);
        env.setStateBackend(rocksDBStateBackend);

        // avro schema




    }
}
