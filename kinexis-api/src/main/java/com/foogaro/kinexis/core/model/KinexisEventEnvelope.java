package com.foogaro.kinexis.core.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.foogaro.kinexis.core.Misc.EVENT_CONTENT_KEY;
import static com.foogaro.kinexis.core.Misc.EVENT_OPERATION_KEY;
import static com.foogaro.kinexis.core.model.KinexisEvent.EVENT_ENTITY_TYPE_KEY;
import static com.foogaro.kinexis.core.model.KinexisEvent.EVENT_ID_KEY;
import static com.foogaro.kinexis.core.model.KinexisEvent.EVENT_SCHEMA_VERSION_KEY;

public record KinexisEventEnvelope(Map<String, String> record) {

    public KinexisEventEnvelope {
        record = Map.copyOf(Objects.requireNonNull(record, "record cannot be null"));
    }

    public static KinexisEventEnvelope from(Map<String, String> record, String fallbackRecordId, Class<?> fallbackEntityType) {
        Map<String, String> values = new HashMap<>(Objects.requireNonNull(record, "record cannot be null"));
        if (fallbackRecordId != null && !fallbackRecordId.isBlank()) {
            values.putIfAbsent(EVENT_ID_KEY, fallbackRecordId);
        }
        if (fallbackEntityType != null) {
            values.putIfAbsent(EVENT_ENTITY_TYPE_KEY, fallbackEntityType.getName());
        }
        values.putIfAbsent(EVENT_SCHEMA_VERSION_KEY, KinexisEvent.CURRENT_SCHEMA_VERSION);
        return new KinexisEventEnvelope(values);
    }

    public String eventId() {
        return record.get(EVENT_ID_KEY);
    }

    public String entityType() {
        return record.get(EVENT_ENTITY_TYPE_KEY);
    }

    public String schemaVersion() {
        return Optional.ofNullable(record.get(EVENT_SCHEMA_VERSION_KEY))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .orElse(KinexisEvent.CURRENT_SCHEMA_VERSION);
    }

    public String operation() {
        return record.get(EVENT_OPERATION_KEY);
    }

    public String content() {
        return record.get(EVENT_CONTENT_KEY);
    }

    public List<String> targets() {
        return KinexisEvent.targets(record);
    }

    public KinexisEventEnvelope with(String key, String value) {
        Map<String, String> values = toRecordMap();
        if (value == null) {
            values.remove(key);
        } else {
            values.put(key, value);
        }
        return new KinexisEventEnvelope(values);
    }

    public KinexisEventEnvelope withContent(String content) {
        return with(EVENT_CONTENT_KEY, content);
    }

    public KinexisEventEnvelope withSchemaVersion(String schemaVersion) {
        return with(EVENT_SCHEMA_VERSION_KEY, schemaVersion);
    }

    public Map<String, String> toRecordMap() {
        return new HashMap<>(record);
    }
}
