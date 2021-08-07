package org.example.app.kafka.func;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.example.app.kafka.bean.Metalog;
import org.example.app.kafka.bean.TimeInfo;
import org.example.util.TimeUtils;

public class GetTimeInfoFromMetaLogAll extends ProcessFunction<Metalog, TimeInfo> {
    @Override
    public void processElement(Metalog value, ProcessFunction<Metalog, TimeInfo>.Context ctx, Collector<TimeInfo> out) throws Exception {
        final int id = value.getPartitionId();
        final long recordTimeStamp = value.getTime();
        final String recordTime = TimeUtils.formatDateTimeFromTimestamp(recordTimeStamp);
        final String currentWM = TimeUtils.formatDateTimeFromTimestamp(ctx.timerService().currentWatermark());
        final TimeInfo timeInfo = new TimeInfo(id, recordTime, currentWM);
        out.collect(timeInfo);
    }
}
