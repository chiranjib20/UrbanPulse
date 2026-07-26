package com.urbanpulse.flink.functions;

import com.urbanpulse.flink.model.IncidentAlert;
import com.urbanpulse.flink.model.TrafficSignalEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;

/**
 * Task C / Problem 9(b) - Traffic Gridlock.
 * Keyed by junction_id. Maintains a running count of CONSECUTIVE signal
 * cycles whose avg_wait_sec exceeded 180s, and the running sum of their
 * wait times (for the alert message). Any cycle at or below the threshold
 * resets the streak - "consecutive" is enforced by resetting on a miss,
 * not just counting breaches in a rolling window.
 */
public class GridlockDetector extends KeyedProcessFunction<String, TrafficSignalEvent, IncidentAlert> {

    private static final double WAIT_THRESHOLD_SEC = 180.0;
    private static final int CONSECUTIVE_CYCLES_REQUIRED = 3;

    private transient ValueState<Integer> consecutiveBreaches;
    private transient ValueState<Double> breachWaitSum;
    private transient ValueState<Boolean> alreadyAlerted; // avoid re-alerting every cycle once triggered

    @Override
    public void open(Configuration parameters) {
        consecutiveBreaches = getRuntimeContext().getState(
                new ValueStateDescriptor<>("consecutiveBreaches", Integer.class, 0));
        breachWaitSum = getRuntimeContext().getState(
                new ValueStateDescriptor<>("breachWaitSum", Double.class, 0.0));
        alreadyAlerted = getRuntimeContext().getState(
                new ValueStateDescriptor<>("alreadyAlerted", Boolean.class, false));
    }

    @Override
    public void processElement(TrafficSignalEvent event, Context ctx, Collector<IncidentAlert> out) throws Exception {
        if (event.avg_wait_sec == null) return;

        long eventTime = ctx.timestamp() != null ? ctx.timestamp() : event.eventTimeMillis();

        if (event.avg_wait_sec > WAIT_THRESHOLD_SEC) {
            int count = consecutiveBreaches.value() + 1;
            double sum = breachWaitSum.value() + event.avg_wait_sec;
            consecutiveBreaches.update(count);
            breachWaitSum.update(sum);

            if (count >= CONSECUTIVE_CYCLES_REQUIRED && !alreadyAlerted.value()) {
                double avgOverStreak = sum / count;
                out.collect(IncidentAlert.gridlock(
                        event.junction_id, event.zone, avgOverStreak, Instant.ofEpochMilli(eventTime).toString()));
                alreadyAlerted.update(true); // suppress repeat alerts while streak continues
            }
        } else {
            // Streak broken - reset for the next potential gridlock episode.
            consecutiveBreaches.update(0);
            breachWaitSum.update(0.0);
            alreadyAlerted.update(false);
        }
    }
}
