package com.foogaro.kinexis.core.telemetry;

import java.time.Duration;
import java.util.Map;

public interface KinexisTelemetry {

    String STREAM_EVENTS_PUBLISHED = "kinexis.stream.events.published";
    String STREAM_EVENTS_PROCESSED = "kinexis.stream.events.processed";
    String STORE_WRITE_LATENCY = "kinexis.store.write.latency";
    String STORE_FAILURES = "kinexis.store.failures";
    String PENDING_RETRIES = "kinexis.pending.retries";
    String DLQ_RECORDS = "kinexis.dlq.records";
    String DLQ_REPLAYS = "kinexis.dlq.replays";
    String CACHE_HITS = "kinexis.cache.hits";
    String CACHE_MISSES = "kinexis.cache.misses";

    void increment(String name, Map<String, String> tags);

    void recordDuration(String name, Duration duration, Map<String, String> tags);

    default KinexisTelemetrySnapshot snapshot() {
        return KinexisTelemetrySnapshot.empty();
    }
}
