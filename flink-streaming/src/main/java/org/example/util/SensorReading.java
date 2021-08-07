package org.example.util;

/**
 * POJO to hold sensor reading data.
 */
public class SensorReading {

    // id of the sensor
    public String id;
    // timestamp of the reading
    public long timestamp;
    // temperature value of the reading
    public double temperature;

    /**
     * Empty default constructor to satify Flink's POJO requirements.
     */
    public SensorReading() { }

    public SensorReading(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "(" + this.id + ", " + this.timestamp + ", " + this.temperature + ")";
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }
}
