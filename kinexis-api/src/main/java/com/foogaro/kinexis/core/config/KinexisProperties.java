package com.foogaro.kinexis.core.config;

import java.time.Duration;

public class KinexisProperties {

    private final Stream stream = new Stream();
    private final Stores stores = new Stores();
    private final Processing processing = new Processing();
    private final Validation validation = new Validation();

    public Stream getStream() {
        return stream;
    }

    public Stores getStores() {
        return stores;
    }

    public Processing getProcessing() {
        return processing;
    }

    public Validation getValidation() {
        return validation;
    }

    public static class Stores {

        private final RepositoryDiscovery repositoryDiscovery = new RepositoryDiscovery();

        public RepositoryDiscovery getRepositoryDiscovery() {
            return repositoryDiscovery;
        }
    }

    public static class RepositoryDiscovery {

        private boolean enabled = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class Processing {

        private int maxParallelStores = Math.max(2, Runtime.getRuntime().availableProcessors());
        private final Idempotency idempotency = new Idempotency();
        private final Ordering ordering = new Ordering();
        private final Backpressure backpressure = new Backpressure();

        public int getMaxParallelStores() {
            return maxParallelStores;
        }

        public void setMaxParallelStores(int maxParallelStores) {
            this.maxParallelStores = maxParallelStores;
        }

        public Idempotency getIdempotency() {
            return idempotency;
        }

        public Ordering getOrdering() {
            return ordering;
        }

        public Backpressure getBackpressure() {
            return backpressure;
        }
    }

    public static class Idempotency {

        private boolean enabled = true;
        private Duration retention = Duration.ofDays(7);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getRetention() {
            return retention;
        }

        public void setRetention(Duration retention) {
            this.retention = retention;
        }
    }

    public static class Ordering {

        private boolean perEntityEnabled = true;

        public boolean isPerEntityEnabled() {
            return perEntityEnabled;
        }

        public void setPerEntityEnabled(boolean perEntityEnabled) {
            this.perEntityEnabled = perEntityEnabled;
        }
    }

    public static class Backpressure {

        private int maxInFlightPerStream = 0;
        private int executorQueueSize = 1024;
        private QueueFullBehavior queueFullBehavior = QueueFullBehavior.BLOCK;
        private Duration slowDownDelay = Duration.ofMillis(100);

        public int getMaxInFlightPerStream() {
            return maxInFlightPerStream;
        }

        public void setMaxInFlightPerStream(int maxInFlightPerStream) {
            this.maxInFlightPerStream = maxInFlightPerStream;
        }

        public int getExecutorQueueSize() {
            return executorQueueSize;
        }

        public void setExecutorQueueSize(int executorQueueSize) {
            this.executorQueueSize = executorQueueSize;
        }

        public QueueFullBehavior getQueueFullBehavior() {
            return queueFullBehavior;
        }

        public void setQueueFullBehavior(QueueFullBehavior queueFullBehavior) {
            this.queueFullBehavior = queueFullBehavior;
        }

        public Duration getSlowDownDelay() {
            return slowDownDelay;
        }

        public void setSlowDownDelay(Duration slowDownDelay) {
            this.slowDownDelay = slowDownDelay;
        }
    }

    public enum QueueFullBehavior {
        BLOCK,
        REJECT_TO_DLQ,
        SLOW_DOWN
    }

    public static class Validation {

        private boolean enabled = true;
        private boolean failFast = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isFailFast() {
            return failFast;
        }

        public void setFailFast(boolean failFast) {
            this.failFast = failFast;
        }
    }

    public static class Stream {

        private Duration pollTimeout = Duration.ofSeconds(1);
        private int batchSize = 100;
        private int partitions = 1;
        private final Listener listener = new Listener();

        public Duration getPollTimeout() {
            return pollTimeout;
        }

        public void setPollTimeout(Duration pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getPartitions() {
            return partitions;
        }

        public void setPartitions(int partitions) {
            this.partitions = partitions;
        }

        public Listener getListener() {
            return listener;
        }
    }

    public static class Listener {

        private final Pending pending = new Pending();

        public Pending getPending() {
            return pending;
        }
    }

    public static class Pending {

        private int maxAttempts = 3;
        private long maxRetention = 120000;
        private int batchSize = 50;
        private long fixedDelay = 300000;

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public long getMaxRetention() {
            return maxRetention;
        }

        public void setMaxRetention(long maxRetention) {
            this.maxRetention = maxRetention;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public long getFixedDelay() {
            return fixedDelay;
        }

        public void setFixedDelay(long fixedDelay) {
            this.fixedDelay = fixedDelay;
        }
    }
}
