package org.example.app.window;

import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class MyProcessFunction extends ProcessWindowFunction<Tuple2<String,Long>,String,String, TimeWindow> {


    public MyProcessFunction(ValueStateDescriptor<Long> globalStateDescriptor) {
         this.globalStateDescriptor = globalStateDescriptor;
    }

    ValueStateDescriptor<Long> windowStateDescriptor;
    ValueStateDescriptor<Long> globalStateDescriptor;

    @Override
    public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
        final KeyedStateStore globalState = context.globalState();
        final KeyedStateStore windowState = context.windowState();
        final ValueState<Long> windowStateState = windowState.getState(windowStateDescriptor);
        final ValueState<Long> globalStateState = globalState.getState(globalStateDescriptor);


        long len = 0;
        final Iterator<Tuple2<String, Long>> iterator = elements.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            len++;
        }

        if(windowStateState.value() == null){
            windowStateState.update(len);
        }else{
            windowStateState.update(len + windowStateState.value());
        }


        if(globalStateState.value() == null){
            globalStateState.update(1L);
        }else{
            globalStateState.update(1L + globalStateState.value());
        }

        out.collect("current windowStateState Value: " + windowStateState.value() + "\tcurrent globalStateState Value: " + globalStateState.value());
    }

    @Override
    public void clear(ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context context) throws Exception {
        super.clear(context);
        context.windowState().getState(windowStateDescriptor).clear();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        windowStateDescriptor = new ValueStateDescriptor<Long>("per-window",Long.class);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
