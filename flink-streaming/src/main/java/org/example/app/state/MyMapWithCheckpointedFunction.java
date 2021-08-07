package org.example.app.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

public class MyMapWithCheckpointedFunction implements MapFunction<Tuple2<String,Integer>, String>, CheckpointedFunction {
    int threshold ;

    ValueState<Integer> keyedCunt;
    ListState<Integer> operatorCunt;
    int localCunt;



    public MyMapWithCheckpointedFunction(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public String map(Tuple2<String,Integer> value) throws Exception {
        // init
        if(keyedCunt.value() != null){
            System.out.println("not init , value: " + keyedCunt.value());
        }else{
            System.out.println("keyedCunt value is null");
            keyedCunt.update(0);
        }

        if(value.f1 > threshold){
            localCunt ++ ;
            keyedCunt.update(keyedCunt.value() + 1);
        }
        return value.f1 +"|"+ localCunt +"|"+ keyedCunt.value() + "";
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        operatorCunt.clear();
        operatorCunt.add(localCunt);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        final OperatorStateStore operatorStateStore = context.getOperatorStateStore();
        operatorCunt = operatorStateStore.getListState(new ListStateDescriptor<Integer>("operator",Integer.class));

        final KeyedStateStore keyedStateStore = context.getKeyedStateStore();
        keyedCunt = keyedStateStore.getState(new ValueStateDescriptor<>("keyed",Integer.class));

        for (Integer integer : operatorCunt.get()) {
            localCunt += integer;
        }
        System.out.println("localCunt: " + localCunt);
    }
}
