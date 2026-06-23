package com.foogaro.kinexis.core.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kinexis")
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

        public int getMaxParallelStores() {
            return maxParallelStores;
        }

        public void setMaxParallelStores(int maxParallelStores) {
            this.maxParallelStores = maxParallelStores;
        }
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
