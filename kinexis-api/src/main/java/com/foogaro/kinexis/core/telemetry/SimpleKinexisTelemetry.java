package com.foogaro.kinexis.core.telemetry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class SimpleKinexisTelemetry implements KinexisTelemetry {

    private final ConcurrentMap<MetricKey, LongAdder> counters = new ConcurrentHashMap<>();
    private final ConcurrentMap<MetricKey, TimerState> timers = new ConcurrentHashMap<>();
    private final ConcurrentMap<MetricKey, AtomicLong> gauges = new ConcurrentHashMap<>();

    @Override
    public void increment(String name, Map<String, String> tags) {
        counters.computeIfAbsent(MetricKey.of(name, tags), ignored -> new LongAdder()).increment();
    }

    @Override
    public void recordDuration(String name, Duration duration, Map<String, String> tags) {
        if (duration == null || duration.isNegative()) {
            return;
        }
        timers.computeIfAbsent(MetricKey.of(name, tags), ignored -> new TimerState())
                .record(duration.toNanos());
    }

    @Override
    public void recordGauge(String name, long value, Map<String, String> tags) {
        gauges.computeIfAbsent(MetricKey.of(name, tags), ignored -> new AtomicLong())
                .set(value);
    }

    @Override
    public KinexisTelemetrySnapshot snapshot() {
        ArrayList<KinexisTelemetrySnapshot.CounterSample> counterSamples = new ArrayList<>();
        counters.forEach((key, value) -> counterSamples.add(
                new KinexisTelemetrySnapshot.CounterSample(key.name(), key.tags(), value.sum())));
        counterSamples.sort(Comparator.comparing(KinexisTelemetrySnapshot.CounterSample::name)
                .thenComparing(sample -> sample.tags().toString()));

        ArrayList<KinexisTelemetrySnapshot.TimerSample> timerSamples = new ArrayList<>();
        timers.forEach((key, value) -> timerSamples.add(
                new KinexisTelemetrySnapshot.TimerSample(
                        key.name(),
                        key.tags(),
                        value.count.sum(),
                        value.totalNanos.sum(),
                        value.maxNanos.get())));
        timerSamples.sort(Comparator.comparing(KinexisTelemetrySnapshot.TimerSample::name)
                .thenComparing(sample -> sample.tags().toString()));

        ArrayList<KinexisTelemetrySnapshot.GaugeSample> gaugeSamples = new ArrayList<>();
        gauges.forEach((key, value) -> gaugeSamples.add(
                new KinexisTelemetrySnapshot.GaugeSample(key.name(), key.tags(), value.get())));
        gaugeSamples.sort(Comparator.comparing(KinexisTelemetrySnapshot.GaugeSample::name)
                .thenComparing(sample -> sample.tags().toString()));

        return new KinexisTelemetrySnapshot(counterSamples, timerSamples, gaugeSamples);
    }

    private record MetricKey(String name, Map<String, String> tags) {

        private static MetricKey of(String name, Map<String, String> tags) {
            return new MetricKey(name, new TreeMap<>(tags == null ? Map.of() : tags));
        }
    }

    private static final class TimerState {

        private final LongAdder count = new LongAdder();
        private final LongAdder totalNanos = new LongAdder();
        private final AtomicLong maxNanos = new AtomicLong();

        private void record(long nanos) {
            count.increment();
            totalNanos.add(nanos);
            maxNanos.accumulateAndGet(nanos, Math::max);
        }
    }
}
