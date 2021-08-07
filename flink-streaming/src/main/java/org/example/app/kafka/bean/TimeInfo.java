package org.example.app.kafka.bean;

public class TimeInfo {
    private int partitionId;
    private String recordTime;
    private String watermark;

    public TimeInfo(int partitionId, String recordTime, String watermark) {
        this.partitionId = partitionId;
        this.recordTime = recordTime;
        this.watermark = watermark;
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

    @Override
    public String toString() {
        return "TimeInfo{" +
                "partitionId=" + partitionId +
                ", recordTime='" + recordTime + '\'' +
                ", watermark='" + watermark + '\'' +
                '}';
    }
}
