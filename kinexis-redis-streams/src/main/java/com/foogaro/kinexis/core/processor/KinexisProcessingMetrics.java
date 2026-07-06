package com.foogaro.kinexis.core.processor;

import com.foogaro.kinexis.core.telemetry.KinexisTelemetry;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class KinexisProcessingMetrics {

    private static final KinexisTelemetry NOOP_TELEMETRY = new KinexisTelemetry() {
        @Override
        public void increment(String name, Map<String, String> tags) {
        }

        @Override
        public void recordDuration(String name, java.time.Duration duration, Map<String, String> tags) {
        }
    };

    private final AtomicInteger storeExecutorQueueDepth = new AtomicInteger();
    private final AtomicInteger storeExecutorActiveWorkers = new AtomicInteger();
    private final AtomicLong storeTasksSubmitted = new AtomicLong();
    private final AtomicLong storeTasksCompleted = new AtomicLong();
    private final AtomicLong storeTasksFailed = new AtomicLong();
    private final AtomicLong backpressureRejections = new AtomicLong();
    private final AtomicLong backpressureSlowdowns = new AtomicLong();
    private final AtomicLong pendingRetries = new AtomicLong();
    private final AtomicLong deadLetteredRecords = new AtomicLong();
    private final KinexisTelemetry telemetry;

    public KinexisProcessingMetrics() {
        this(NOOP_TELEMETRY);
    }

    public KinexisProcessingMetrics(KinexisTelemetry telemetry) {
        this.telemetry = telemetry == null ? NOOP_TELEMETRY : telemetry;
    }

    public void recordStoreTaskSubmitted() {
        storeTasksSubmitted.incrementAndGet();
        telemetry.increment(KinexisTelemetry.PROCESSING_STORE_TASKS_SUBMITTED, Map.of());
    }

    public void recordStoreTaskCompleted() {
        storeTasksCompleted.incrementAndGet();
        telemetry.increment(KinexisTelemetry.PROCESSING_STORE_TASKS_COMPLETED, Map.of());
    }

    public void recordStoreTaskFailed() {
        storeTasksFailed.incrementAndGet();
        telemetry.increment(KinexisTelemetry.PROCESSING_STORE_TASKS_FAILED, Map.of());
    }

    public void recordBackpressureRejection() {
        backpressureRejections.incrementAndGet();
        telemetry.increment(KinexisTelemetry.PROCESSING_BACKPRESSURE_REJECTIONS, Map.of());
    }

    public void recordBackpressureSlowdown() {
        backpressureSlowdowns.incrementAndGet();
        telemetry.increment(KinexisTelemetry.PROCESSING_BACKPRESSURE_SLOWDOWNS, Map.of());
    }

    public void recordPendingRetry() {
        pendingRetries.incrementAndGet();
        telemetry.increment(KinexisTelemetry.PROCESSING_PENDING_RETRIES, Map.of());
    }

    public void recordDeadLetteredRecord() {
        deadLetteredRecords.incrementAndGet();
        telemetry.increment(KinexisTelemetry.PROCESSING_DEAD_LETTERED_RECORDS, Map.of());
    }

    public void updateStoreExecutor(int queueDepth, int activeWorkers) {
        storeExecutorQueueDepth.set(queueDepth);
        storeExecutorActiveWorkers.set(activeWorkers);
        telemetry.recordGauge(KinexisTelemetry.PROCESSING_STORE_EXECUTOR_QUEUE_DEPTH, queueDepth, Map.of());
        telemetry.recordGauge(KinexisTelemetry.PROCESSING_STORE_EXECUTOR_ACTIVE_WORKERS, activeWorkers, Map.of());
    }

    public Snapshot snapshot() {
        return new Snapshot(
                storeExecutorQueueDepth.get(),
                storeExecutorActiveWorkers.get(),
                storeTasksSubmitted.get(),
                storeTasksCompleted.get(),
                storeTasksFailed.get(),
                backpressureRejections.get(),
                backpressureSlowdowns.get(),
                pendingRetries.get(),
                deadLetteredRecords.get());
    }

    public record Snapshot(int storeExecutorQueueDepth,
                           int storeExecutorActiveWorkers,
                           long storeTasksSubmitted,
                           long storeTasksCompleted,
                           long storeTasksFailed,
                           long backpressureRejections,
                           long backpressureSlowdowns,
                           long pendingRetries,
                           long deadLetteredRecords) {
    }
}
