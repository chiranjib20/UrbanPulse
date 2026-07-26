package com.urbanpulse.flink.model;

import java.io.Serializable;
import java.time.Instant;

public class BusGpsEvent implements Serializable {
    public String bus_id;
    public String route_id;
    public Double lat;
    public Double lon;
    public Double speed_kmh;
    public Integer occupancy_pct;
    public String timestamp;

    public long eventTimeMillis() {
        return Instant.parse(timestamp).toEpochMilli();
    }
}
