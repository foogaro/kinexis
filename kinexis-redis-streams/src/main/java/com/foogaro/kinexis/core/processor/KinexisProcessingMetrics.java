package com.foogaro.kinexis.core.processor;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class KinexisProcessingMetrics {

    private final AtomicInteger storeExecutorQueueDepth = new AtomicInteger();
    private final AtomicInteger storeExecutorActiveWorkers = new AtomicInteger();
    private final AtomicLong storeTasksSubmitted = new AtomicLong();
    private final AtomicLong storeTasksCompleted = new AtomicLong();
    private final AtomicLong storeTasksFailed = new AtomicLong();
    private final AtomicLong backpressureRejections = new AtomicLong();
    private final AtomicLong backpressureSlowdowns = new AtomicLong();
    private final AtomicLong pendingRetries = new AtomicLong();
    private final AtomicLong deadLetteredRecords = new AtomicLong();

    public void recordStoreTaskSubmitted() {
        storeTasksSubmitted.incrementAndGet();
    }

    public void recordStoreTaskCompleted() {
        storeTasksCompleted.incrementAndGet();
    }

    public void recordStoreTaskFailed() {
        storeTasksFailed.incrementAndGet();
    }

    public void recordBackpressureRejection() {
        backpressureRejections.incrementAndGet();
    }

    public void recordBackpressureSlowdown() {
        backpressureSlowdowns.incrementAndGet();
    }

    public void recordPendingRetry() {
        pendingRetries.incrementAndGet();
    }

    public void recordDeadLetteredRecord() {
        deadLetteredRecords.incrementAndGet();
    }

    public void updateStoreExecutor(int queueDepth, int activeWorkers) {
        storeExecutorQueueDepth.set(queueDepth);
        storeExecutorActiveWorkers.set(activeWorkers);
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
