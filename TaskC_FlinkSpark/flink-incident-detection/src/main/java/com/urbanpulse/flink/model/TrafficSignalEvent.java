package com.urbanpulse.flink.model;

import java.io.Serializable;
import java.time.Instant;

public class TrafficSignalEvent implements Serializable {
    public String junction_id;
    public String zone;
    public Integer vehicle_count;
    public Double avg_wait_sec;
    public String signal_phase;
    public String timestamp;

    public long eventTimeMillis() {
        return Instant.parse(timestamp).toEpochMilli();
    }
}
