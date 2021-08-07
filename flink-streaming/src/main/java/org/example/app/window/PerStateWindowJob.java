package org.example.app.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.util.TimeUtils;

import java.time.Duration;
import java.util.Properties;

public class PerStateWindowJob {
    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        final Properties props = new Properties();
//        env.getConfig().setAutoWatermarkInterval(1);
        env.setParallelism(1);

        final SingleOutputStreamOperator<Tuple2<String, Long>> mapRes = env.socketTextStream("controller", 5555)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        final String[] split = value.split(",");
                        if (split.length == 2) {
                            Long time = TimeUtils.getTimestampFromDateTime(split[1]);
                            return Tuple2.of(split[0], time);
                        } else {
                            return Tuple2.of("error_key", -1L);
                        }
                    }
                });

        final BasicTypeInfo<Integer> intTypeInfo = BasicTypeInfo.INT_TYPE_INFO;


        mapRes.print();

        final WatermarkStrategy<Tuple2<String, Long>> watermarkStrategy = WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                });

        final SingleOutputStreamOperator<Tuple2<String, Long>> withWaterMark = mapRes.assignTimestampsAndWatermarks(watermarkStrategy);

        final OutputTag<Tuple2<String,Long>> lateOutputTag = new OutputTag<Tuple2<String,Long>>("late-data"){};
        ValueStateDescriptor<Long> globalStateDescriptor = new ValueStateDescriptor<Long>("global",Long.class);

        withWaterMark
                .keyBy(ele -> ele.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(10))
                .sideOutputLateData(lateOutputTag)
                .process(new MyProcessFunction(globalStateDescriptor))
                .print();


        env.execute();
    }
}


