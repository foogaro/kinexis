package com.foogaro.kinexis.core.telemetry;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class CompositeKinexisTelemetry implements KinexisTelemetry {

    private final List<KinexisTelemetry> delegates;

    public CompositeKinexisTelemetry(List<KinexisTelemetry> delegates) {
        this.delegates = List.copyOf(delegates);
    }

    @Override
    public void increment(String name, Map<String, String> tags) {
        delegates.forEach(delegate -> delegate.increment(name, tags));
    }

    @Override
    public void recordDuration(String name, Duration duration, Map<String, String> tags) {
        delegates.forEach(delegate -> delegate.recordDuration(name, duration, tags));
    }

    @Override
    public KinexisTelemetrySnapshot snapshot() {
        return delegates.stream()
                .map(KinexisTelemetry::snapshot)
                .filter(snapshot -> !snapshot.counters().isEmpty() || !snapshot.timers().isEmpty())
                .findFirst()
                .orElseGet(KinexisTelemetrySnapshot::empty);
    }
}
