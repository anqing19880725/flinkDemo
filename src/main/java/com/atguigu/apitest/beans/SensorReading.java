package com.atguigu.apitest.beans;


// 传感器温度读数的数据类型
public class SensorReading {
    // 属性:id,时间戳,温度值
    private String id;
    private Long timestamp;
    private Double temprature;

    public SensorReading() {
    }

    public SensorReading(String id, Long timestamp, Double temprature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temprature = temprature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemprature() {
        return temprature;
    }

    public void setTemprature(Double temprature) {
        this.temprature = temprature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temprature=" + temprature +
                '}';
    }
}
