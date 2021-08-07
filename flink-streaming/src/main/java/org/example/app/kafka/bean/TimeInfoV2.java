package org.example.app.kafka.bean;

public class TimeInfoV2 {
    private int partitionId;
    private String recordTime;
    private String watermark;
    private String curWatermark;

    public TimeInfoV2(int partitionId, String recordTime, String watermark, String curWatermark) {
        this.partitionId = partitionId;
        this.recordTime = recordTime;
        this.watermark = watermark;
        this.curWatermark = curWatermark;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public String getRecordTime() {
        return recordTime;
    }

    public void setRecordTime(String recordTime) {
        this.recordTime = recordTime;
    }

    public String getWatermark() {
        return watermark;
    }

    public void setWatermark(String watermark) {
        this.watermark = watermark;
    }

    public String getCurWatermark() {
        return curWatermark;
    }

    public void setCurWatermark(String curWatermark) {
        this.curWatermark = curWatermark;
    }

    @Override
    public String toString() {
        return "TimeInfoV2{" +
                "partitionId=" + partitionId +
                ", recordTime='" + recordTime + '\'' +
                ", watermark='" + watermark + '\'' +
                ", curWatermark='" + curWatermark + '\'' +
                '}';
    }
}
