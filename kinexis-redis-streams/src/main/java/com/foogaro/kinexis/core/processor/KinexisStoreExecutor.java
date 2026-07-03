package com.foogaro.kinexis.core.processor;

import com.foogaro.kinexis.core.config.KinexisProperties;
import com.foogaro.kinexis.core.exception.KinexisBackpressureException;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class KinexisStoreExecutor extends AbstractExecutorService {

    private final ThreadPoolExecutor delegate;
    private final KinexisProperties.Backpressure backpressure;
    private final KinexisProcessingMetrics metrics;

    public KinexisStoreExecutor(int parallelism,
                                int queueSize,
                                KinexisProperties.Backpressure backpressure,
                                KinexisProcessingMetrics metrics,
                                ThreadFactory threadFactory) {
        this.backpressure = Objects.requireNonNull(backpressure, "backpressure cannot be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null");
        this.delegate = new ThreadPoolExecutor(
                parallelism,
                parallelism,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(Math.max(1, queueSize)),
                Objects.requireNonNull(threadFactory, "threadFactory cannot be null"),
                new ThreadPoolExecutor.AbortPolicy());
        updateExecutorMetrics();
    }

    @Override
    public void execute(Runnable command) {
        Runnable measuredCommand = measured(command);
        try {
            delegate.execute(measuredCommand);
            metrics.recordStoreTaskSubmitted();
            updateExecutorMetrics();
        } catch (RejectedExecutionException e) {
            handleRejected(measuredCommand, e);
        }
    }

    private void handleRejected(Runnable command, RejectedExecutionException cause) {
        KinexisProperties.QueueFullBehavior behavior = backpressure.getQueueFullBehavior();
        if (behavior == KinexisProperties.QueueFullBehavior.REJECT_TO_DLQ) {
            metrics.recordBackpressureRejection();
            updateExecutorMetrics();
            throw new RejectedExecutionException(new KinexisBackpressureException("Kinexis store executor queue is full", cause));
        }
        if (behavior == KinexisProperties.QueueFullBehavior.SLOW_DOWN) {
            slowDownAndEnqueue(command);
            return;
        }
        blockAndEnqueue(command);
    }

    private void blockAndEnqueue(Runnable command) {
        try {
            delegate.getQueue().put(command);
            metrics.recordStoreTaskSubmitted();
            updateExecutorMetrics();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.recordBackpressureRejection();
            throw new RejectedExecutionException(new KinexisBackpressureException("Interrupted while waiting for Kinexis store executor queue capacity", e));
        }
    }

    private void slowDownAndEnqueue(Runnable command) {
        Duration delay = backpressure.getSlowDownDelay();
        long sleepMillis = delay == null || delay.isNegative() ? 0L : delay.toMillis();
        while (!delegate.isShutdown()) {
            metrics.recordBackpressureSlowdown();
            sleep(sleepMillis);
            if (delegate.getQueue().offer(command)) {
                metrics.recordStoreTaskSubmitted();
                updateExecutorMetrics();
                return;
            }
        }
        metrics.recordBackpressureRejection();
        throw new RejectedExecutionException(new KinexisBackpressureException("Kinexis store executor is shut down"));
    }

    private Runnable measured(Runnable command) {
        return () -> {
            updateExecutorMetrics();
            try {
                command.run();
                metrics.recordStoreTaskCompleted();
            } catch (RuntimeException e) {
                metrics.recordStoreTaskFailed();
                throw e;
            } finally {
                updateExecutorMetrics();
            }
        };
    }

    private void sleep(long sleepMillis) {
        if (sleepMillis <= 0L) {
            Thread.yield();
            return;
        }
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metrics.recordBackpressureRejection();
            throw new RejectedExecutionException(new KinexisBackpressureException("Interrupted while slowing Kinexis store executor submission", e));
        }
    }

    private void updateExecutorMetrics() {
        metrics.updateStoreExecutor(delegate.getQueue().size(), delegate.getActiveCount());
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }
}
