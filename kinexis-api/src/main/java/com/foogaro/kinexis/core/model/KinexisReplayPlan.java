package com.foogaro.kinexis.core.model;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public record KinexisReplayPlan(
        String entityType,
        String failedStore,
        int recordsFound,
        List<String> targetStores,
        List<String> storesAlreadyProcessed,
        List<String> storesUnhealthy,
        List<Record> records) {

    public KinexisReplayPlan {
        records = records == null ? List.of() : List.copyOf(records);
        recordsFound = records.size();
        targetStores = distinct(targetStores);
        storesAlreadyProcessed = distinct(storesAlreadyProcessed);
        storesUnhealthy = distinct(storesUnhealthy);
    }

    public int recordsSkipped() {
        return count(Record::wouldSkip);
    }

    public int recordsReplayed() {
        return count(Record::wouldReplay);
    }

    public int recordsRequiringSchemaUpcast() {
        return count(Record::requiresSchemaUpcast);
    }

    public List<Record> skippedRecords() {
        return matchingRecords(Record::wouldSkip);
    }

    public List<Record> replayedRecords() {
        return matchingRecords(Record::wouldReplay);
    }

    public List<Record> recordsRequiringUpcast() {
        return matchingRecords(Record::requiresSchemaUpcast);
    }

    private int count(Predicate<Record> predicate) {
        return (int) records.stream().filter(predicate).count();
    }

    private List<Record> matchingRecords(Predicate<Record> predicate) {
        return records.stream().filter(predicate).toList();
    }

    private static List<String> distinct(Collection<String> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        LinkedHashSet<String> normalized = new LinkedHashSet<>();
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                normalized.add(value.trim());
            }
        }
        return List.copyOf(normalized);
    }

    public record Record(
            String recordId,
            String eventId,
            String entityId,
            String operation,
            String failedStore,
            List<String> targetStores,
            List<String> alreadyProcessedStores,
            List<String> unhealthyStores,
            boolean wouldSkip,
            String skipReason,
            boolean wouldReplay,
            boolean requiresSchemaUpcast,
            String fromSchemaVersion,
            String toSchemaVersion) {

        public Record {
            recordId = Objects.toString(recordId, "");
            eventId = Objects.toString(eventId, "");
            entityId = Objects.toString(entityId, "");
            operation = Objects.toString(operation, "");
            failedStore = Objects.toString(failedStore, "");
            targetStores = distinct(targetStores);
            alreadyProcessedStores = distinct(alreadyProcessedStores);
            unhealthyStores = distinct(unhealthyStores);
            skipReason = Objects.toString(skipReason, "");
            fromSchemaVersion = Objects.toString(fromSchemaVersion, "");
            toSchemaVersion = Objects.toString(toSchemaVersion, "");
        }
    }
}
