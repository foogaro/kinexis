package com.foogaro.kinexis.core.telemetry;

import org.springframework.beans.factory.ListableBeanFactory;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public class MicrometerKinexisTelemetry implements KinexisTelemetry {

    private final Object meterRegistry;

    public MicrometerKinexisTelemetry(Object meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public static Optional<MicrometerKinexisTelemetry> from(ListableBeanFactory beanFactory) {
        try {
            Class<?> meterRegistryType = Class.forName("io.micrometer.core.instrument.MeterRegistry");
            String[] beanNames = beanFactory.getBeanNamesForType(meterRegistryType);
            if (beanNames.length == 0) {
                return Optional.empty();
            }
            return Optional.of(new MicrometerKinexisTelemetry(beanFactory.getBean(beanNames[0])));
        } catch (ClassNotFoundException ignored) {
            return Optional.empty();
        }
    }

    @Override
    public void increment(String name, Map<String, String> tags) {
        try {
            Method counter = meterRegistry.getClass().getMethod("counter", String.class, String[].class);
            Object meter = counter.invoke(meterRegistry, name, toTagArray(tags));
            meter.getClass().getMethod("increment").invoke(meter);
        } catch (ReflectiveOperationException ignored) {
        }
    }

    @Override
    public void recordDuration(String name, Duration duration, Map<String, String> tags) {
        if (duration == null || duration.isNegative()) {
            return;
        }
        try {
            Class<?> timerClass = Class.forName("io.micrometer.core.instrument.Timer");
            Class<?> meterRegistryClass = Class.forName("io.micrometer.core.instrument.MeterRegistry");
            Object builder = timerClass.getMethod("builder", String.class).invoke(null, name);
            builder = builder.getClass().getMethod("tags", String[].class).invoke(builder, (Object) toTagArray(tags));
            Object timer = builder.getClass().getMethod("register", meterRegistryClass).invoke(builder, meterRegistry);
            timer.getClass().getMethod("record", Duration.class).invoke(timer, duration);
        } catch (ReflectiveOperationException ignored) {
        }
    }

    private String[] toTagArray(Map<String, String> tags) {
        Map<String, String> safeTags = tags == null ? Map.of() : tags;
        String[] values = new String[safeTags.size() * 2];
        int index = 0;
        for (Map.Entry<String, String> entry : safeTags.entrySet()) {
            values[index++] = entry.getKey();
            values[index++] = entry.getValue();
        }
        return values;
    }
}
