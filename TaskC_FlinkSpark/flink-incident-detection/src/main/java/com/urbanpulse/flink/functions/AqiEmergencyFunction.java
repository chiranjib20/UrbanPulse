package com.urbanpulse.flink.functions;

import com.urbanpulse.flink.model.AirQualityEvent;
import com.urbanpulse.flink.model.IncidentAlert;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * Task C / Problem 9(a) - AQI Emergency.
 * Keyed by sensor_id. Emits an alert the moment a reading exceeds AQI > 300
 * (Hazardous) - well inside the 2-minute SLA since this is a stateless
 * threshold check with no windowing delay. ValueState<Long> lastAlertTime
 * prevents alert-flooding: once an emergency is raised for a sensor, we
 * suppress repeats for the same sensor until 2 minutes have passed, so a
 * sensor stuck above 300 doesn't spam the incidents topic every reading.
 */
public class AqiEmergencyFunction extends KeyedProcessFunction<String, AirQualityEvent, IncidentAlert> {

    private static final double AQI_HAZARDOUS_THRESHOLD = 300.0;
    private static final long ALERT_SUPPRESSION_WINDOW_MS = 2 * 60 * 1000L; // 2 minutes

    private transient ValueState<Long> lastAlertTimeState;

    @Override
    public void open(Configuration parameters) {
        lastAlertTimeState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastAlertTime", Long.class));
    }

    @Override
    public void processElement(AirQualityEvent event, Context ctx, Collector<IncidentAlert> out) throws Exception {
        if (event.aqi == null || event.sensor_status != null && event.sensor_status.equals("TIMEOUT")) {
            return; // null/invalid readings are handled by the DLQ validator, not alerted on
        }
        if (event.aqi <= AQI_HAZARDOUS_THRESHOLD) {
            return;
        }

        long eventTime = ctx.timestamp() != null ? ctx.timestamp() : event.eventTimeMillis();
        Long lastAlert = lastAlertTimeState.value();
        if (lastAlert != null && (eventTime - lastAlert) < ALERT_SUPPRESSION_WINDOW_MS) {
            return; // still within suppression window for this sensor
        }

        lastAlertTimeState.update(eventTime);
        out.collect(IncidentAlert.aqiEmergency(
                event.sensor_id, event.zone, event.aqi, Instant.ofEpochMilli(eventTime).toString()));
    }
}
