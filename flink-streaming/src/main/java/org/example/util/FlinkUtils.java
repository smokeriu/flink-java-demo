package org.example.util;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkUtils {

    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }

}
