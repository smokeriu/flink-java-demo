package org.example.app.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Window Join
 * @author liushengwei
 */
public class StateJob2 {
    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // set watermark
        final WatermarkStrategy<Tuple3<Long, Integer, String>> watermarkStrategy = WatermarkStrategy.<Tuple3<Long, Integer, String>>forBoundedOutOfOrderness(Duration.ofMillis(1000))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<Long, Integer, String>>() {
                    @Override
                    public long extractTimestamp(Tuple3<Long, Integer, String> element, long recordTimestamp) {
                        return element.f0;
                    }
                });

        // init data
        final List<Tuple3<Long, Integer, String>> data1 = new ArrayList<>();
        data1.add(Tuple3.of(1612147000L,1,"info1"));
        data1.add(Tuple3.of(1612150000L,2,"info2"));
        data1.add(Tuple3.of(1612149000L,3,"info3"));
        data1.add(Tuple3.of(1612150000L,4,"info4"));
        data1.add(Tuple3.of(1612151000L,5,"info5"));
        data1.add(Tuple3.of(1612131000L,1,"info6"));

        final List<Tuple3<Long, Integer, String>> data2 = new ArrayList<>();
        data2.add(Tuple3.of(1612147000L,1,"info1"));
        data2.add(Tuple3.of(1612148000L,2,"info2"));
        data2.add(Tuple3.of(1612166000L,3,"info3"));
        data2.add(Tuple3.of(1612150000L,4,"info4"));
        data2.add(Tuple3.of(1612151000L,5,"info5"));



        // log_time,key,info1
        final SingleOutputStreamOperator<Tuple3<Long, Integer, String>> input1 = env.fromCollection(data1)
                .assignTimestampsAndWatermarks(watermarkStrategy);
        final SingleOutputStreamOperator<Tuple3<Long, Integer, String>> input2 = env.fromCollection(data2)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        final SingleOutputStreamOperator<String> sum = input1
                .keyBy(data -> data.f1)
                .map(new RichMapFunction<Tuple3<Long, Integer, String>, String>() {

            ValueState<Integer> state;


            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("sum", Integer.class));
            }

            @Override
            public String map(Tuple3<Long, Integer, String> value) throws Exception {
                if (state.value() == null) {
                    state.update(0);
                }
                final int res = value.f1 + state.value();
                state.update(res);
                return value.f2 + "--" +res;
            }
        });

        sum.print();

        env.execute("window Join");

    }
}
