package org.example.app.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * @author liushengwei
 */
public class CheckpointedFunctionStateApp {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);
        env.enableCheckpointing(10000);
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-java-demo-20210216"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        final ParameterTool fromArgs = ParameterTool.fromArgs(args);
        final String hostname = fromArgs.get("hostname","localhost");
        final int port = fromArgs.getInt("port",9999);
        final DataStreamSource<String> input = env.socketTextStream(hostname, port);

        input.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                final String[] splits = value.split(",");
                String f0 = splits[0];
                int f1 = Integer.parseInt(splits[1]);
                return Tuple2.of(f0, f1);
            }
        })
                .keyBy(data -> data.f0)
                .map(new MyMapWithCheckpointedFunction(1))
                .print();

        env.execute();
    }
}
