package com.urbanpulse.flink.functions;

import com.urbanpulse.flink.model.BusGpsEvent;
import com.urbanpulse.flink.model.IncidentAlert;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Task C / Problem 9(c) - Bus Bunching.
 * Keyed by route_id, so this function's state only ever sees buses on the
 * SAME route - exactly the comparison the requirement needs ("two buses on
 * the same route_id"). For every incoming position we:
 *   1. Update that bus's last-known position/time in `busPositions`.
 *   2. Compare it against every other bus on the route last seen within
 *      STALE_AFTER_MS (so we don't compare against a bus that dropped off
 *      the network hours ago).
 *   3. Track, per unordered bus pair, the timestamp proximity (<=200m)
 *      began in `pairCloseSince`. If the pair separates (>200m), the pair's
 *      streak resets - "more than 5 minutes" means a CONTINUOUS window.
 *   4. Once a pair has been continuously close for >= 5 minutes, emit one
 *      alert and latch `pairAlerted` so we don't re-alert every subsequent
 *      position update while the buses remain bunched.
 *
 * Distance uses the haversine formula on lat/lon (good enough at city scale;
 * no need for a full geodesic library for a 200m threshold check).
 */
public class BusBunchingDetector extends KeyedProcessFunction<String, BusGpsEvent, IncidentAlert> {

    private static final double PROXIMITY_METERS = 200.0;
    private static final long BUNCHING_DURATION_MS = 5 * 60 * 1000L; // 5 minutes
    private static final long STALE_AFTER_MS = 3 * 60 * 1000L;       // ignore buses not heard from recently
    private static final double EARTH_RADIUS_M = 6_371_000.0;

    private transient MapState<String, double[]> busPositions;   // busId -> [lat, lon, eventTimeMillis]
    private transient MapState<String, Long> pairCloseSince;     // "busA::busB" -> first continuously-close timestamp
    private transient MapState<String, Boolean> pairAlerted;     // "busA::busB" -> already alerted for this streak

    @Override
    public void open(Configuration parameters) {
        busPositions = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("busPositions", String.class, double[].class));
        pairCloseSince = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("pairCloseSince", String.class, Long.class));
        pairAlerted = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("pairAlerted", String.class, Boolean.class));
    }

    @Override
    public void processElement(BusGpsEvent event, Context ctx, Collector<IncidentAlert> out) throws Exception {
        if (event.lat == null || event.lon == null) return;

        long eventTime = ctx.timestamp() != null ? ctx.timestamp() : event.eventTimeMillis();
        String thisBus = event.bus_id;

        busPositions.put(thisBus, new double[]{event.lat, event.lon, eventTime});

        List<String> otherBuses = new ArrayList<>();
        for (Map.Entry<String, double[]> entry : busPositions.entries()) {
            if (!entry.getKey().equals(thisBus)) otherBuses.add(entry.getKey());
        }

        for (String otherBus : otherBuses) {
            double[] otherPos = busPositions.get(otherBus);
            if (otherPos == null) continue;
            long otherTime = (long) otherPos[2];
            if (Math.abs(eventTime - otherTime) > STALE_AFTER_MS) {
                continue; // other bus's last position is too old to compare meaningfully
            }

            double distance = haversineMeters(event.lat, event.lon, otherPos[0], otherPos[1]);
            String pairKey = pairKey(thisBus, otherBus);

            if (distance <= PROXIMITY_METERS) {
                Long closeSince = pairCloseSince.get(pairKey);
                if (closeSince == null) {
                    pairCloseSince.put(pairKey, eventTime);
                } else {
                    boolean alreadyAlerted = Boolean.TRUE.equals(pairAlerted.get(pairKey));
                    if (!alreadyAlerted && (eventTime - closeSince) >= BUNCHING_DURATION_MS) {
                        out.collect(IncidentAlert.busBunching(
                                event.route_id, thisBus, otherBus, distance,
                                Instant.ofEpochMilli(eventTime).toString()));
                        pairAlerted.put(pairKey, true);
                    }
                }
            } else {
                // Pair separated - reset the continuity streak.
                pairCloseSince.remove(pairKey);
                pairAlerted.remove(pairKey);
            }
        }
    }

    private static String pairKey(String busA, String busB) {
        return busA.compareTo(busB) < 0 ? busA + "::" + busB : busB + "::" + busA;
    }

    private static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS_M * c;
    }
}
