package org.example.app.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.Collections;
import java.util.List;

public class MyOperatorStateMap2 implements MapFunction<Integer, Integer>{
    int tmp = 0;
    @Override
    public Integer map(Integer value) throws Exception {
        tmp += value;
        return tmp;
    }
}
