package com.urbanpulse.flink.model;

import java.io.Serializable;

public class IncidentAlert implements Serializable {
    public String incident_type;   // AQI_EMERGENCY | TRAFFIC_GRIDLOCK | BUS_BUNCHING
    public String junction_id;     // gridlock only
    public String zone;            // aqi + gridlock
    public String sensor_id;       // aqi only
    public Double aqi;             // aqi only
    public String route_id;        // bunching only
    public String bus_id_1;        // bunching only
    public String bus_id_2;        // bunching only
    public double distance_meters; // bunching only
    public String detected_at;
    public String details;

    public IncidentAlert() {}

    public static IncidentAlert aqiEmergency(String sensorId, String zone, double aqi, String detectedAt) {
        IncidentAlert a = new IncidentAlert();
        a.incident_type = "AQI_EMERGENCY";
        a.sensor_id = sensorId;
        a.zone = zone;
        a.aqi = aqi;
        a.detected_at = detectedAt;
        a.details = String.format("Sensor %s in %s reported hazardous AQI=%.0f", sensorId, zone, aqi);
        return a;
    }

    public static IncidentAlert gridlock(String junctionId, String zone, double avgWaitAcrossCycles, String detectedAt) {
        IncidentAlert a = new IncidentAlert();
        a.incident_type = "TRAFFIC_GRIDLOCK";
        a.junction_id = junctionId;
        a.zone = zone;
        a.detected_at = detectedAt;
        a.details = String.format("Junction %s exceeded 180s avg wait for 3 consecutive cycles (avg=%.0fs)", junctionId, avgWaitAcrossCycles);
        return a;
    }

    public static IncidentAlert busBunching(String routeId, String bus1, String bus2, double distanceMeters, String detectedAt) {
        IncidentAlert a = new IncidentAlert();
        a.incident_type = "BUS_BUNCHING";
        a.route_id = routeId;
        a.bus_id_1 = bus1;
        a.bus_id_2 = bus2;
        a.distance_meters = distanceMeters;
        a.detected_at = detectedAt;
        a.details = String.format("Buses %s and %s on route %s within %.0fm for >5 minutes", bus1, bus2, routeId, distanceMeters);
        return a;
    }
}
