package org.example.app.state;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class BroadcastStateApp {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // data1
        final List<Tuple3<Long, Integer, String>> data1 = new ArrayList<>();
        data1.add(Tuple3.of(1612147000L,1,"info1"));
        data1.add(Tuple3.of(1612150000L,2,"info2"));
        data1.add(Tuple3.of(1612149000L,3,"info3"));
        data1.add(Tuple3.of(1612150000L,4,"info4"));
        data1.add(Tuple3.of(1612151000L,5,"info5"));
        data1.add(Tuple3.of(1612131000L,1,"info6"));
        data1.add(Tuple3.of(1612131000L,1,"info7"));
        data1.add(Tuple3.of(1612131000L,1,"info8"));
        data1.add(Tuple3.of(1612131000L,3,"info9"));

        // data2
        final List<Integer> data2 = new ArrayList<>();
        data2.add(1);
        data2.add(2);
        data2.add(3);
        data2.add(4);
        data2.add(5);
        data2.add(6);


        final DataStreamSource<Tuple3<Long, Integer, String>> input1 = env.fromCollection(data1);

        // broadcast state descriptor , must be MapDescriptor
        final MapStateDescriptor<String, Integer> stateDescriptor = new MapStateDescriptor<>("broadcast", String.class, Integer.class);

        final BroadcastStream<Integer> broadcast = env.fromCollection(data2)
                .broadcast(stateDescriptor);
        input1.keyBy(data -> data.f1)
                .connect(broadcast)
                .process(new KeyedBroadcastProcessFunction<Integer, Tuple3<Long, Integer, String>, Integer, String>() {
                    final MapStateDescriptor<String, Integer> stateDescriptor =
                            new MapStateDescriptor<>("broadcast", String.class, Integer.class);

                    ValueState<Integer> lastState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        final ValueStateDescriptor<Integer> last = new ValueStateDescriptor<>("last", Integer.class);
                        lastState = getRuntimeContext().getState(last);
                    }

                    @Override
                    public void processElement(Tuple3<Long, Integer, String> value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        final ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(stateDescriptor);
                        if(broadcastState.contains(value.f1.toString())){
                            lastState.update(value.f1);
                            out.collect(value.f2);
                        }
                    }

                    @Override
                    public void processBroadcastElement(Integer value, Context ctx, Collector<String> out) throws Exception {
                        final BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(stateDescriptor);
                        // 只能这样访问分区state
                        ctx.applyToKeyedState(new ValueStateDescriptor<>("last", Integer.class), new KeyedStateFunction<Integer, ValueState<Integer>>() {
                            @Override
                            public void process(Integer key, ValueState<Integer> state) throws Exception {
                                if(state.value() != null){

                                }
                            }
                        });
                        // 不能这样访问分区state
//                        if(lastState.value() != null){
//                            System.out.println(lastState.value());
//                        }
                        if(value <= 3){
                            broadcastState.put(value.toString(),1);
                        }
                    }

                }).print();

        env.execute("broadcast");
    }
}
