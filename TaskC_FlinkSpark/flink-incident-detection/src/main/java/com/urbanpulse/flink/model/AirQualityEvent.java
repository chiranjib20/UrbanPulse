package com.urbanpulse.flink.model;

import java.io.Serializable;
import java.time.Instant;

public class AirQualityEvent implements Serializable {
    public String sensor_id;
    public String zone;
    public Double pm25;
    public Double pm10;
    public Double no2;
    public Double aqi;
    public String sensor_status;
    public String timestamp;

    public long eventTimeMillis() {
        return Instant.parse(timestamp).toEpochMilli();
    }
}
