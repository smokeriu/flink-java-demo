package org.example.app.kafka.bean;

public class Metalog {
    private int id;
    private String ln1;
    private String sip;
    private String sp;
    private String dip;
    private String dp;
    private long time;
    private int partitionId;


    public Metalog(int id, String ln1, String sip, String sp, String dip, String dp, long time, int partitionId) {
        this.id = id;
        this.ln1 = ln1;
        this.sip = sip;
        this.sp = sp;
        this.dip = dip;
        this.dp = dp;
        this.time = time;
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getLn1() {
        return ln1;
    }

    public void setLn1(String ln1) {
        this.ln1 = ln1;
    }

    public String getSip() {
        return sip;
    }

    public void setSip(String sip) {
        this.sip = sip;
    }

    public String getSp() {
        return sp;
    }

    public void setSp(String sp) {
        this.sp = sp;
    }

    public String getDip() {
        return dip;
    }

    public void setDip(String dip) {
        this.dip = dip;
    }

    public String getDp() {
        return dp;
    }

    public void setDp(String dp) {
        this.dp = dp;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
