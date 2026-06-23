package com.foogaro.kinexis.core.model;

import com.foogaro.kinexis.core.Misc;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.foogaro.kinexis.core.Misc.EVENT_CONTENT_KEY;
import static com.foogaro.kinexis.core.Misc.EVENT_OPERATION_KEY;

public final class KinexisEvent {

    public static final String EVENT_ENTITY_TYPE_KEY = "entityType";
    public static final String EVENT_SCHEMA_VERSION_KEY = "schemaVersion";
    public static final String EVENT_TIMESTAMP_KEY = "timestamp";
    public static final String EVENT_TARGETS_KEY = "targets";
    public static final String CURRENT_SCHEMA_VERSION = "1";

    private final String entityType;
    private final Misc.Operation operation;
    private final String content;
    private final Instant timestamp;
    private final List<String> targets;

    public KinexisEvent(String entityType, Misc.Operation operation, String content, Instant timestamp) {
        this(entityType, operation, content, timestamp, List.of());
    }

    public KinexisEvent(String entityType, Misc.Operation operation, String content, Instant timestamp, Collection<String> targets) {
        this.entityType = Objects.requireNonNull(entityType, "entityType cannot be null");
        this.operation = Objects.requireNonNull(operation, "operation cannot be null");
        this.content = Objects.requireNonNull(content, "content cannot be null");
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp cannot be null");
        this.targets = List.copyOf(Objects.requireNonNull(targets, "targets cannot be null"));
    }

    public static KinexisEvent save(Class<?> entityClass, String content) {
        return new KinexisEvent(entityClass.getName(), Misc.Operation.SAVE, content, Instant.now());
    }

    public static KinexisEvent save(Class<?> entityClass, String content, String... targets) {
        return new KinexisEvent(entityClass.getName(), Misc.Operation.SAVE, content, Instant.now(), targets(targets));
    }

    public static KinexisEvent delete(Class<?> entityClass, Object id) {
        return new KinexisEvent(entityClass.getName(), Misc.Operation.DELETE, String.valueOf(id), Instant.now());
    }

    public static KinexisEvent delete(Class<?> entityClass, Object id, String... targets) {
        return new KinexisEvent(entityClass.getName(), Misc.Operation.DELETE, String.valueOf(id), Instant.now(), targets(targets));
    }

    public static List<String> targets(Map<String, String> record) {
        String value = record.get(EVENT_TARGETS_KEY);
        if (value == null || value.isBlank()) {
            return List.of();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(target -> !target.isEmpty())
                .toList();
    }

    private static List<String> targets(String... targets) {
        if (targets == null || targets.length == 0) {
            return List.of();
        }
        return Arrays.stream(targets)
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(target -> !target.isEmpty())
                .toList();
    }

    public String entityType() {
        return entityType;
    }

    public Misc.Operation operation() {
        return operation;
    }

    public String content() {
        return content;
    }

    public Instant timestamp() {
        return timestamp;
    }

    public List<String> targets() {
        return targets;
    }

    public Map<String, String> toRecordMap() {
        Map<String, String> map = new HashMap<>();
        map.put(EVENT_ENTITY_TYPE_KEY, entityType);
        map.put(EVENT_OPERATION_KEY, operation.getValue());
        map.put(EVENT_CONTENT_KEY, content);
        map.put(EVENT_TIMESTAMP_KEY, timestamp.toString());
        map.put(EVENT_SCHEMA_VERSION_KEY, CURRENT_SCHEMA_VERSION);
        if (!targets.isEmpty()) {
            map.put(EVENT_TARGETS_KEY, String.join(",", targets));
        }
        return map;
    }
}
