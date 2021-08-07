package org.example.app.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
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
public class WindowJoinJob2 {
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

        final DataStream<String> out = input1.join(input2)
                .where(left -> left.f1)
                .equalTo(right -> right.f1)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2000)))
                .apply(new JoinFunction<Tuple3<Long, Integer, String>, Tuple3<Long, Integer, String>, String>() {
                    @Override
                    public String join(Tuple3<Long, Integer, String> first, Tuple3<Long, Integer, String> second) throws Exception {
                        return first.f2 + second.f2;
                    }
                });

        out.print();



        env.execute("window Join");

    }
}
