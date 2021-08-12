package org.example.util;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * flink 工具类
 *
 * @author liushengwei
 * @date 2021/08/09
 */
public class FlinkUtils {

    private FlinkUtils(){}

    /**
     * env
     */
    private static final StreamExecutionEnvironment ENV = StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     * 得到env
     *
     * @return {@link StreamExecutionEnvironment}
     */
    public static StreamExecutionEnvironment getEnv() {
        return ENV;
    }

}
