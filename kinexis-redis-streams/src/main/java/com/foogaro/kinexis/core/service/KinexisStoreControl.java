package com.foogaro.kinexis.core.service;

import com.foogaro.kinexis.core.config.KinexisProperties;
import com.foogaro.kinexis.core.exception.KinexisStoreUnavailableException;
import com.foogaro.kinexis.core.model.KinexisStoreHealthState;
import com.foogaro.kinexis.core.model.KinexisStoreHealthStatus;
import com.foogaro.kinexis.core.telemetry.KinexisTelemetry;
import com.foogaro.kinexis.core.telemetry.SimpleKinexisTelemetry;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KinexisStoreControl {

    private final KinexisProperties properties;
    private final KinexisTelemetry telemetry;
    private final Clock clock;
    private final ConcurrentMap<StoreKey, StoreState> states = new ConcurrentHashMap<>();

    public KinexisStoreControl(KinexisProperties properties) {
        this(properties, new SimpleKinexisTelemetry());
    }

    public KinexisStoreControl(KinexisProperties properties, KinexisTelemetry telemetry) {
        this(properties, telemetry, Clock.systemUTC());
    }

    KinexisStoreControl(KinexisProperties properties, KinexisTelemetry telemetry, Clock clock) {
        this.properties = Objects.requireNonNull(properties, "properties cannot be null");
        this.telemetry = Objects.requireNonNull(telemetry, "telemetry cannot be null");
        this.clock = Objects.requireNonNull(clock, "clock cannot be null");
    }

    public void pause(Class<?> entityType, String storeName) {
        updateState(entityType, storeName, KinexisStoreHealthState.PAUSED, KinexisTelemetry.STORE_PAUSED);
    }

    public void resume(Class<?> entityType, String storeName) {
        StoreKey key = key(entityType, storeName);
        StoreState state = states.computeIfAbsent(key, ignored -> new StoreState());
        synchronized (state) {
            state.reset();
            transition(key, state, KinexisStoreHealthState.ACTIVE);
        }
        telemetry.increment(KinexisTelemetry.STORE_RESUMED, tags(key));
    }

    public void degrade(Class<?> entityType, String storeName) {
        updateState(entityType, storeName, KinexisStoreHealthState.DEGRADED, null);
    }

    public void openCircuit(Class<?> entityType, String storeName) {
        StoreKey key = key(entityType, storeName);
        StoreState state = states.computeIfAbsent(key, ignored -> new StoreState());
        synchronized (state) {
            openCircuit(key, state, now());
        }
    }

    public KinexisStoreHealthStatus status(Class<?> entityType, String storeName) {
        StoreKey key = key(entityType, storeName);
        StoreState state = states.get(key);
        if (state == null) {
            return new KinexisStoreHealthStatus(entityType.getSimpleName(), storeName,
                    KinexisStoreHealthState.ACTIVE, 0, 0, null, null);
        }
        synchronized (state) {
            refreshOpenCircuit(key, state, now());
            return status(key, state);
        }
    }

    public List<KinexisStoreHealthStatus> status(Class<?> entityType) {
        String entityName = entityType.getName();
        return states.entrySet().stream()
                .filter(entry -> entry.getKey().entityType().equals(entityName))
                .map(entry -> {
                    synchronized (entry.getValue()) {
                        refreshOpenCircuit(entry.getKey(), entry.getValue(), now());
                        return status(entry.getKey(), entry.getValue());
                    }
                })
                .sorted(Comparator.comparing(KinexisStoreHealthStatus::storeName))
                .toList();
    }

    public List<KinexisStoreHealthStatus> statuses() {
        return states.entrySet().stream()
                .map(entry -> {
                    synchronized (entry.getValue()) {
                        refreshOpenCircuit(entry.getKey(), entry.getValue(), now());
                        return status(entry.getKey(), entry.getValue());
                    }
                })
                .sorted(Comparator.comparing(KinexisStoreHealthStatus::entityType)
                        .thenComparing(KinexisStoreHealthStatus::storeName))
                .toList();
    }

    public void beforeCall(Class<?> entityType, String storeName) {
        if (!properties.getStoreHealth().isEnabled()) {
            return;
        }
        StoreKey key = key(entityType, storeName);
        StoreState state = states.computeIfAbsent(key, ignored -> new StoreState());
        synchronized (state) {
            Instant now = now();
            refreshOpenCircuit(key, state, now);
            if (state.state == KinexisStoreHealthState.PAUSED) {
                throw new KinexisStoreUnavailableException(storeName, "Store " + storeName + " is paused");
            }
            if (state.state == KinexisStoreHealthState.OPEN_CIRCUIT) {
                throw new KinexisStoreUnavailableException(storeName, "Circuit is open for store " + storeName);
            }
        }
    }

    public void recordSuccess(Class<?> entityType, String storeName) {
        if (!properties.getStoreHealth().isEnabled()) {
            return;
        }
        StoreKey key = key(entityType, storeName);
        StoreState state = states.computeIfAbsent(key, ignored -> new StoreState());
        synchronized (state) {
            if (state.state == KinexisStoreHealthState.RECOVERING || state.state == KinexisStoreHealthState.DEGRADED) {
                boolean recovering = state.state == KinexisStoreHealthState.RECOVERING;
                state.probeSuccesses++;
                telemetry.increment(KinexisTelemetry.STORE_PROBE_SUCCESSES, tags(key));
                if (state.probeSuccesses >= probeSuccessThreshold()) {
                    state.reset();
                    transition(key, state, KinexisStoreHealthState.ACTIVE);
                    if (recovering) {
                        telemetry.increment(KinexisTelemetry.STORE_CIRCUIT_CLOSED, tags(key));
                    }
                }
                return;
            }
            if (state.state == KinexisStoreHealthState.ACTIVE) {
                pruneFailures(state, now());
            }
        }
    }

    public void recordFailure(Class<?> entityType, String storeName, Throwable cause) {
        if (!properties.getStoreHealth().isEnabled()) {
            return;
        }
        StoreKey key = key(entityType, storeName);
        StoreState state = states.computeIfAbsent(key, ignored -> new StoreState());
        synchronized (state) {
            Instant now = now();
            refreshOpenCircuit(key, state, now);
            state.lastFailureAt = now;
            state.probeSuccesses = 0;
            state.failures.addLast(now);
            pruneFailures(state, now);
            if (state.state == KinexisStoreHealthState.RECOVERING) {
                telemetry.increment(KinexisTelemetry.STORE_PROBE_FAILURES, tags(key));
                openCircuit(key, state, now);
                return;
            }
            if (state.state == KinexisStoreHealthState.ACTIVE || state.state == KinexisStoreHealthState.DEGRADED) {
                if (state.failures.size() >= failureThreshold()) {
                    openCircuit(key, state, now);
                } else if (state.state == KinexisStoreHealthState.ACTIVE) {
                    transition(key, state, KinexisStoreHealthState.DEGRADED);
                }
            }
        }
    }

    private void updateState(Class<?> entityType, String storeName, KinexisStoreHealthState nextState, String counter) {
        StoreKey key = key(entityType, storeName);
        StoreState state = states.computeIfAbsent(key, ignored -> new StoreState());
        synchronized (state) {
            transition(key, state, nextState);
        }
        if (counter != null) {
            telemetry.increment(counter, tags(key));
        }
    }

    private void openCircuit(StoreKey key, StoreState state, Instant now) {
        Duration openDuration = properties.getStoreHealth().getOpenDuration();
        state.openedUntil = openDuration == null || openDuration.isNegative()
                ? now
                : now.plus(openDuration);
        transition(key, state, KinexisStoreHealthState.OPEN_CIRCUIT);
        telemetry.increment(KinexisTelemetry.STORE_CIRCUIT_OPENED, tags(key));
    }

    private void refreshOpenCircuit(StoreKey key, StoreState state, Instant now) {
        if (state.state == KinexisStoreHealthState.OPEN_CIRCUIT
                && state.openedUntil != null
                && !state.openedUntil.isAfter(now)) {
            state.probeSuccesses = 0;
            transition(key, state, KinexisStoreHealthState.RECOVERING);
        }
    }

    private void transition(StoreKey key, StoreState state, KinexisStoreHealthState nextState) {
        if (state.state == nextState) {
            recordStateGauge(key, nextState);
            return;
        }
        KinexisStoreHealthState previous = state.state;
        state.state = nextState;
        recordStateGauge(key, previous, 0);
        recordStateGauge(key, nextState, 1);
    }

    private void recordStateGauge(StoreKey key, KinexisStoreHealthState state) {
        recordStateGauge(key, state, 1);
    }

    private void recordStateGauge(StoreKey key, KinexisStoreHealthState state, long value) {
        Map<String, String> tags = tags(key, state);
        telemetry.recordGauge(KinexisTelemetry.STORE_HEALTH_STATE, value, tags);
    }

    private void pruneFailures(StoreState state, Instant now) {
        Duration failureWindow = properties.getStoreHealth().getFailureWindow();
        if (failureWindow == null || failureWindow.isZero() || failureWindow.isNegative()) {
            return;
        }
        Instant cutoff = now.minus(failureWindow);
        while (!state.failures.isEmpty() && state.failures.peekFirst().isBefore(cutoff)) {
            state.failures.removeFirst();
        }
    }

    private KinexisStoreHealthStatus status(StoreKey key, StoreState state) {
        return new KinexisStoreHealthStatus(
                key.entitySimpleName(),
                key.storeName(),
                state.state,
                state.failures.size(),
                state.probeSuccesses,
                state.openedUntil,
                state.lastFailureAt);
    }

    private StoreKey key(Class<?> entityType, String storeName) {
        Objects.requireNonNull(entityType, "entityType cannot be null");
        if (storeName == null || storeName.isBlank()) {
            throw new IllegalArgumentException("storeName cannot be blank");
        }
        return new StoreKey(entityType.getName(), entityType.getSimpleName(), storeName);
    }

    private Instant now() {
        return clock.instant();
    }

    private int failureThreshold() {
        return Math.max(1, properties.getStoreHealth().getFailureThreshold());
    }

    private int probeSuccessThreshold() {
        return Math.max(1, properties.getStoreHealth().getProbeSuccessThreshold());
    }

    private Map<String, String> tags(StoreKey key) {
        return Map.of("entity", key.entitySimpleName(), "store", key.storeName());
    }

    private Map<String, String> tags(StoreKey key, KinexisStoreHealthState state) {
        return Map.of("entity", key.entitySimpleName(), "store", key.storeName(), "state", state.name());
    }

    private record StoreKey(String entityType, String entitySimpleName, String storeName) {
    }

    private static final class StoreState {

        private KinexisStoreHealthState state = KinexisStoreHealthState.ACTIVE;
        private final ArrayDeque<Instant> failures = new ArrayDeque<>();
        private int probeSuccesses;
        private Instant openedUntil;
        private Instant lastFailureAt;

        private void reset() {
            failures.clear();
            probeSuccesses = 0;
            openedUntil = null;
            lastFailureAt = null;
        }
    }
}
