package com.foogaro.kinexis.core.telemetry;

import java.util.List;
import java.util.Map;

public record KinexisTelemetrySnapshot(List<CounterSample> counters, List<TimerSample> timers) {

    public KinexisTelemetrySnapshot {
        counters = List.copyOf(counters);
        timers = List.copyOf(timers);
    }

    public static KinexisTelemetrySnapshot empty() {
        return new KinexisTelemetrySnapshot(List.of(), List.of());
    }

    public record CounterSample(String name, Map<String, String> tags, long count) {
        public CounterSample {
            tags = Map.copyOf(tags);
        }
    }

    public record TimerSample(String name,
                              Map<String, String> tags,
                              long count,
                              long totalNanos,
                              long maxNanos) {
        public TimerSample {
            tags = Map.copyOf(tags);
        }
    }
}
