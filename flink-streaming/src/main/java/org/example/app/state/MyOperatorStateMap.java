package org.example.app.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.Collections;
import java.util.List;

public class MyOperatorStateMap implements MapFunction<Integer, Integer>, ListCheckpointed<Integer> {

    int tmp = 0;


    @Override
    public Integer map(Integer value) throws Exception {
        tmp += value;
        return tmp;
    }

    @Override
    public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
        System.out.println("snap!");
        return Collections.singletonList(tmp);
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
        System.out.println("restore!");
        for (Integer integer : state) {
            System.out.println(integer);
            tmp += integer;
        }
    }
}
