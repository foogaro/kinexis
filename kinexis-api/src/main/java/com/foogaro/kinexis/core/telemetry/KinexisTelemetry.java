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
    String DLQ_REPLAY_FAILURES = "kinexis.dlq.replay.failures";
    String CACHE_HITS = "kinexis.cache.hits";
    String CACHE_MISSES = "kinexis.cache.misses";
    String PROCESSING_STORE_TASKS_SUBMITTED = "kinexis.processing.store.tasks.submitted";
    String PROCESSING_STORE_TASKS_COMPLETED = "kinexis.processing.store.tasks.completed";
    String PROCESSING_STORE_TASKS_FAILED = "kinexis.processing.store.tasks.failed";
    String PROCESSING_BACKPRESSURE_REJECTIONS = "kinexis.processing.backpressure.rejections";
    String PROCESSING_BACKPRESSURE_SLOWDOWNS = "kinexis.processing.backpressure.slowdowns";
    String PROCESSING_PENDING_RETRIES = "kinexis.processing.pending.retries";
    String PROCESSING_DEAD_LETTERED_RECORDS = "kinexis.processing.deadletter.records";
    String PROCESSING_STORE_EXECUTOR_QUEUE_DEPTH = "kinexis.processing.store.executor.queue.depth";
    String PROCESSING_STORE_EXECUTOR_ACTIVE_WORKERS = "kinexis.processing.store.executor.active.workers";
    String STORE_HEALTH_STATE = "kinexis.store.health.state";
    String STORE_CIRCUIT_OPENED = "kinexis.store.circuit.opened";
    String STORE_CIRCUIT_CLOSED = "kinexis.store.circuit.closed";
    String STORE_PAUSED = "kinexis.store.paused";
    String STORE_RESUMED = "kinexis.store.resumed";
    String STORE_PROBE_FAILURES = "kinexis.store.probe.failures";
    String STORE_PROBE_SUCCESSES = "kinexis.store.probe.successes";

    void increment(String name, Map<String, String> tags);

    void recordDuration(String name, Duration duration, Map<String, String> tags);

    default void recordGauge(String name, long value, Map<String, String> tags) {
    }

    default KinexisTelemetrySnapshot snapshot() {
        return KinexisTelemetrySnapshot.empty();
    }
}
