package com.foogaro.kinexis.core.service;

import com.foogaro.kinexis.core.model.KinexisDlqRecord;
import com.foogaro.kinexis.core.model.KinexisEvent;
import com.foogaro.kinexis.core.model.KinexisEventEnvelope;
import com.foogaro.kinexis.core.model.KinexisReplayOptions;
import com.foogaro.kinexis.core.model.KinexisReplayPlan;
import com.foogaro.kinexis.core.model.KinexisReplayResult;
import com.foogaro.kinexis.core.model.KinexisReplayStatus;
import com.foogaro.kinexis.core.model.KinexisStoreHealthStatus;
import com.foogaro.kinexis.core.model.ReplayBatchOptions;
import com.foogaro.kinexis.core.processor.KinexisProcessingCoordinator;
import com.foogaro.kinexis.core.telemetry.KinexisTelemetry;
import com.foogaro.kinexis.core.telemetry.SimpleKinexisTelemetry;
import com.foogaro.kinexis.core.config.KinexisProperties;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static com.foogaro.kinexis.core.Misc.EVENT_CONTENT_KEY;
import static com.foogaro.kinexis.core.Misc.EVENT_OPERATION_KEY;
import static com.foogaro.kinexis.core.Misc.getDLQStreamKey;
import static com.foogaro.kinexis.core.Misc.getStreamKey;
import static com.foogaro.kinexis.core.model.KinexisEvent.EVENT_ENTITY_ID_KEY;
import static com.foogaro.kinexis.core.model.KinexisEvent.EVENT_ENTITY_TYPE_KEY;
import static com.foogaro.kinexis.core.model.KinexisEvent.EVENT_ID_KEY;
import static com.foogaro.kinexis.core.model.KinexisEvent.EVENT_TARGETS_KEY;
import static com.foogaro.kinexis.core.model.KinexisEvent.newEventId;
import static com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler.DLQ_ATTEMPTS_KEY;
import static com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler.DLQ_CONSUMER_KEY;
import static com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler.DLQ_ERROR_KEY;
import static com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler.DLQ_EXCEPTION_CLASS_KEY;
import static com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler.DLQ_FAILED_STORE_KEY;
import static com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler.DLQ_FAILURE_TIMESTAMP_KEY;
import static com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler.DLQ_GROUP_KEY;
import static com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler.DLQ_REASON_KEY;
import static com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler.DLQ_STREAM_ID_KEY;
import static com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler.DLQ_STREAM_KEY;

public class KinexisDlqService {

    public enum ReplayMode {
        REPLAY_ONLY,
        REPLAY_AND_DELETE
    }

    private final RedisTemplate<String, String> redisTemplate;
    private final KinexisTelemetry telemetry;
    private final KinexisStoreControl storeControl;
    private final KinexisEventSchemaRegistry eventSchemaRegistry;
    private final KinexisProcessingCoordinator processingCoordinator;

    public KinexisDlqService(RedisTemplate<String, String> redisTemplate) {
        this(redisTemplate, new SimpleKinexisTelemetry());
    }

    public KinexisDlqService(RedisTemplate<String, String> redisTemplate, KinexisTelemetry telemetry) {
        this(redisTemplate, telemetry, new KinexisStoreControl(new KinexisProperties(), telemetry));
    }

    public KinexisDlqService(RedisTemplate<String, String> redisTemplate,
                             KinexisTelemetry telemetry,
                             KinexisStoreControl storeControl) {
        this(redisTemplate, telemetry, storeControl, KinexisEventSchemaRegistry.noop());
    }

    public KinexisDlqService(RedisTemplate<String, String> redisTemplate,
                             KinexisTelemetry telemetry,
                             KinexisStoreControl storeControl,
                             KinexisEventSchemaRegistry eventSchemaRegistry) {
        this(redisTemplate, telemetry, storeControl, eventSchemaRegistry, null);
    }

    public KinexisDlqService(RedisTemplate<String, String> redisTemplate,
                             KinexisTelemetry telemetry,
                             KinexisStoreControl storeControl,
                             KinexisEventSchemaRegistry eventSchemaRegistry,
                             KinexisProcessingCoordinator processingCoordinator) {
        this.redisTemplate = redisTemplate;
        this.telemetry = telemetry;
        this.storeControl = storeControl;
        this.eventSchemaRegistry = eventSchemaRegistry;
        this.processingCoordinator = processingCoordinator;
    }

    public Optional<String> replay(Class<?> entityType, String dlqRecordId) {
        return replay(entityType, dlqRecordId, ReplayMode.REPLAY_ONLY);
    }

    public Optional<String> replay(Class<?> entityType, String dlqRecordId, String... targets) {
        return replay(entityType, dlqRecordId, ReplayMode.REPLAY_ONLY, targets);
    }

    public Optional<String> replay(Class<?> entityType, String dlqRecordId, ReplayMode replayMode, String... targets) {
        return replay(entityType, dlqRecordId, replayMode, false, targets);
    }

    public Optional<String> replayWithNewEventId(Class<?> entityType, String dlqRecordId) {
        return replayWithNewEventId(entityType, dlqRecordId, ReplayMode.REPLAY_ONLY);
    }

    public Optional<String> replayWithNewEventId(Class<?> entityType, String dlqRecordId, String... targets) {
        return replayWithNewEventId(entityType, dlqRecordId, ReplayMode.REPLAY_ONLY, targets);
    }

    public Optional<String> replayWithNewEventId(Class<?> entityType, String dlqRecordId, ReplayMode replayMode, String... targets) {
        return replay(entityType, dlqRecordId, replayMode, true, targets);
    }

    public Optional<String> replayFailedStore(Class<?> entityType, String dlqRecordId) {
        return replayFailedStore(entityType, dlqRecordId, ReplayMode.REPLAY_ONLY);
    }

    public Optional<String> replayFailedStore(Class<?> entityType, String dlqRecordId, ReplayMode replayMode) {
        return replayFailedStore(entityType, dlqRecordId, replayMode, KinexisReplayOptions.safe());
    }

    public Optional<String> replayFailedStore(Class<?> entityType,
                                              String dlqRecordId,
                                              ReplayMode replayMode,
                                              KinexisReplayOptions options) {
        Optional<MapRecord<String, Object, Object>> record = findRecord(entityType, dlqRecordId);
        if (record.isEmpty()) {
            recordReplayFailure(entityType, null, replayMode, false, "notFound");
            return Optional.empty();
        }
        String failedStore = value(record.get(), DLQ_FAILED_STORE_KEY);
        String[] targets = failedStore == null || failedStore.isBlank() ? new String[0] : new String[]{failedStore};
        Optional<String> unhealthyReason = unhealthyReplayReason(entityType, failedStore, options);
        if (unhealthyReason.isPresent()) {
            recordReplayFailure(entityType, record.get(), replayMode, false, unhealthyReason.get(), targets);
            return Optional.empty();
        }
        return replayRecordSafely(getDLQStreamKey(entityType), entityType, record.get(), replayMode, false, targets);
    }

    public KinexisReplayResult replayFailedStoreResult(Class<?> entityType, String dlqRecordId) {
        return replayFailedStoreResult(entityType, dlqRecordId, ReplayMode.REPLAY_ONLY, KinexisReplayOptions.safe());
    }

    public KinexisReplayResult replayFailedStoreResult(Class<?> entityType,
                                                       String dlqRecordId,
                                                       ReplayMode replayMode) {
        return replayFailedStoreResult(entityType, dlqRecordId, replayMode, KinexisReplayOptions.safe());
    }

    public KinexisReplayResult replayFailedStoreResult(Class<?> entityType,
                                                       String dlqRecordId,
                                                       ReplayMode replayMode,
                                                       KinexisReplayOptions options) {
        Optional<MapRecord<String, Object, Object>> record = findRecord(entityType, dlqRecordId);
        if (record.isEmpty()) {
            recordReplayFailure(entityType, null, replayMode, false, "notFound");
            return KinexisReplayResult.notFound(dlqRecordId);
        }
        String failedStore = value(record.get(), DLQ_FAILED_STORE_KEY);
        String[] targets = failedStore == null || failedStore.isBlank() ? new String[0] : new String[]{failedStore};
        Optional<String> unhealthyReason = unhealthyReplayReason(entityType, failedStore, options);
        if (unhealthyReason.isPresent()) {
            recordReplayFailure(entityType, record.get(), replayMode, false, unhealthyReason.get(), targets);
            return KinexisReplayResult.skipped(dlqRecordId, failedStore, unhealthyReason.get());
        }
        try {
            Optional<String> replayedId = replayRecordSafely(getDLQStreamKey(entityType), entityType, record.get(), replayMode, false, targets);
            return replayedId
                    .map(id -> KinexisReplayResult.replayed(dlqRecordId, id, failedStore))
                    .orElseGet(() -> KinexisReplayResult.failed(dlqRecordId, failedStore, "appendReturnedNull"));
        } catch (RuntimeException e) {
            return KinexisReplayResult.failed(dlqRecordId, failedStore, e.getClass().getName());
        }
    }

    public List<String> replayAllFailedStores(Class<?> entityType) {
        return replayAllFailedStores(entityType, ReplayMode.REPLAY_ONLY);
    }

    public List<String> replayAllFailedStores(Class<?> entityType, ReplayMode replayMode) {
        return replayAllHealthyFailedStores(entityType, replayMode).stream()
                .filter(result -> result.status() == KinexisReplayStatus.REPLAYED)
                .map(KinexisReplayResult::replayedRecordId)
                .toList();
    }

    public List<KinexisReplayResult> replayAllHealthyFailedStores(Class<?> entityType) {
        return replayAllHealthyFailedStores(entityType, ReplayMode.REPLAY_ONLY);
    }

    public List<KinexisReplayResult> replayAllHealthyFailedStores(Class<?> entityType, ReplayMode replayMode) {
        return list(entityType).stream()
                .filter(record -> record.failedStore() != null && !record.failedStore().isBlank())
                .map(record -> replayFailedStoreResult(entityType, record.recordId(), replayMode))
                .toList();
    }

    public List<String> replayByStore(Class<?> entityType, String failedStore) {
        return replayByStore(entityType, failedStore, ReplayMode.REPLAY_ONLY);
    }

    public List<String> replayByStore(Class<?> entityType, String failedStore, ReplayMode replayMode) {
        return replayByStoreIfHealthy(entityType, failedStore, replayMode).stream()
                .filter(result -> result.status() == KinexisReplayStatus.REPLAYED)
                .map(KinexisReplayResult::replayedRecordId)
                .toList();
    }

    public List<KinexisReplayResult> replayByStoreIfHealthy(Class<?> entityType, String failedStore) {
        return replayByStoreIfHealthy(entityType, failedStore, ReplayMode.REPLAY_ONLY);
    }

    public List<KinexisReplayResult> replayByStoreIfHealthy(Class<?> entityType, String failedStore, ReplayMode replayMode) {
        return listByFailedStore(entityType, failedStore).stream()
                .map(record -> replayFailedStoreResult(entityType, record.recordId(), replayMode))
                .toList();
    }

    public List<KinexisReplayResult> replayByStore(Class<?> entityType,
                                                   String failedStore,
                                                   ReplayMode replayMode,
                                                   KinexisReplayOptions options) {
        return listByFailedStore(entityType, failedStore).stream()
                .map(record -> replayFailedStoreResult(entityType, record.recordId(), replayMode, options))
                .toList();
    }

    public List<KinexisReplayResult> replayByStore(Class<?> entityType,
                                                   String failedStore,
                                                   ReplayBatchOptions options) {
        ReplayBatchOptions batchOptions = options == null ? ReplayBatchOptions.defaults() : options;
        ReplayMode replayMode = batchOptions.deleteAfterReplay() ? ReplayMode.REPLAY_AND_DELETE : ReplayMode.REPLAY_ONLY;
        List<MapRecord<String, Object, Object>> matchingRecords = records(entityType).stream()
                .filter(record -> failedStore != null && failedStore.equals(value(record, DLQ_FAILED_STORE_KEY)))
                .filter(record -> olderThan(record, batchOptions.olderThan()))
                .limit(batchOptions.limit())
                .toList();
        List<KinexisReplayResult> results = new ArrayList<>();
        for (int index = 0; index < matchingRecords.size(); index++) {
            MapRecord<String, Object, Object> record = matchingRecords.get(index);
            KinexisReplayResult result = replayFailedStoreResult(
                    entityType,
                    record.getId().getValue(),
                    replayMode,
                    batchOptions.replayOptions());
            results.add(result);
            if (batchOptions.stopOnFirstFailure() && result.status() != KinexisReplayStatus.REPLAYED) {
                break;
            }
            if (index < matchingRecords.size() - 1) {
                delay(batchOptions.delayBetweenRecords());
            }
        }
        return List.copyOf(results);
    }

    public KinexisReplayPlan previewReplayByStore(Class<?> entityType, String failedStore) {
        return previewReplayByStore(entityType, failedStore, KinexisReplayOptions.safe());
    }

    public KinexisReplayPlan previewReplayByStore(Class<?> entityType,
                                                  String failedStore,
                                                  KinexisReplayOptions options) {
        List<MapRecord<String, Object, Object>> matchingRecords = records(entityType).stream()
                .filter(record -> failedStore != null && failedStore.equals(value(record, DLQ_FAILED_STORE_KEY)))
                .toList();
        return previewPlan(entityType, failedStore, matchingRecords, options);
    }

    public List<KinexisDlqRecord> list(Class<?> entityType) {
        return records(entityType).stream()
                .map(this::toDlqRecord)
                .toList();
    }

    public List<KinexisDlqRecord> listByFailedStore(Class<?> entityType, String failedStore) {
        return list(entityType, record -> failedStore != null && failedStore.equals(record.failedStore()));
    }

    public List<KinexisDlqRecord> listByOperation(Class<?> entityType, String operation) {
        return list(entityType, record -> operation != null && operation.equals(record.operation()));
    }

    public List<KinexisDlqRecord> listOlderThan(Class<?> entityType, Duration age) {
        if (age == null) {
            return list(entityType);
        }
        Instant threshold = Instant.now().minus(age);
        return list(entityType, record -> record.failureTimestamp() != null && record.failureTimestamp().isBefore(threshold));
    }

    public List<KinexisDlqRecord> list(Class<?> entityType, String failedStore, String operation, Duration olderThan) {
        Instant threshold = olderThan == null ? null : Instant.now().minus(olderThan);
        return list(entityType, record ->
                (failedStore == null || failedStore.equals(record.failedStore()))
                        && (operation == null || operation.equals(record.operation()))
                        && (threshold == null || (record.failureTimestamp() != null && record.failureTimestamp().isBefore(threshold))));
    }

    private List<KinexisDlqRecord> list(Class<?> entityType, Predicate<KinexisDlqRecord> filter) {
        return list(entityType).stream()
                .filter(filter)
                .toList();
    }

    private KinexisReplayPlan previewPlan(Class<?> entityType,
                                          String failedStore,
                                          List<MapRecord<String, Object, Object>> records,
                                          KinexisReplayOptions options) {
        List<KinexisReplayPlan.Record> recordPlans = records.stream()
                .map(record -> previewRecord(entityType, record, failedStore, options))
                .toList();
        return new KinexisReplayPlan(
                entityType.getName(),
                failedStore,
                recordPlans.size(),
                aggregate(recordPlans, KinexisReplayPlan.Record::targetStores),
                aggregate(recordPlans, KinexisReplayPlan.Record::alreadyProcessedStores),
                aggregate(recordPlans, KinexisReplayPlan.Record::unhealthyStores),
                recordPlans);
    }

    private KinexisReplayPlan.Record previewRecord(Class<?> entityType,
                                                   MapRecord<String, Object, Object> record,
                                                   String failedStore,
                                                   KinexisReplayOptions options) {
        Map<String, String> replayMessage = toReplayMessage(record);
        List<String> targetStores = previewTargetStores(record, failedStore, replayMessage);
        List<String> alreadyProcessedStores = alreadyProcessedStores(entityType, replayMessage.get(EVENT_ID_KEY), targetStores);
        Map<String, KinexisStoreHealthStatus> unhealthyStatuses = unhealthyStatuses(entityType, targetStores);
        SchemaPreview schemaPreview = schemaPreview(entityType, record, replayMessage);
        String skipReason = skipReason(replayMessage, targetStores, alreadyProcessedStores, unhealthyStatuses, schemaPreview, options);
        boolean wouldSkip = !skipReason.isBlank();
        return new KinexisReplayPlan.Record(
                record.getId().getValue(),
                replayMessage.get(EVENT_ID_KEY),
                replayMessage.get(EVENT_ENTITY_ID_KEY),
                replayMessage.get(EVENT_OPERATION_KEY),
                value(record, DLQ_FAILED_STORE_KEY),
                targetStores,
                alreadyProcessedStores,
                new ArrayList<>(unhealthyStatuses.keySet()),
                wouldSkip,
                skipReason,
                !wouldSkip,
                schemaPreview.requiresUpcast(),
                schemaPreview.fromVersion(),
                schemaPreview.toVersion());
    }

    private List<String> previewTargetStores(MapRecord<String, Object, Object> record,
                                             String failedStore,
                                             Map<String, String> replayMessage) {
        if (failedStore != null && !failedStore.isBlank()) {
            return List.of(failedStore);
        }
        String recordFailedStore = value(record, DLQ_FAILED_STORE_KEY);
        if (recordFailedStore != null && !recordFailedStore.isBlank()) {
            return List.of(recordFailedStore);
        }
        return KinexisEvent.targets(replayMessage);
    }

    private List<String> alreadyProcessedStores(Class<?> entityType, String eventId, List<String> targetStores) {
        if (processingCoordinator == null || eventId == null || eventId.isBlank()) {
            return List.of();
        }
        return targetStores.stream()
                .filter(store -> processingCoordinator.isProcessed(entityType, eventId, store))
                .toList();
    }

    private Map<String, KinexisStoreHealthStatus> unhealthyStatuses(Class<?> entityType, List<String> targetStores) {
        Map<String, KinexisStoreHealthStatus> statuses = new HashMap<>();
        for (String store : targetStores) {
            KinexisStoreHealthStatus status = storeControl.status(entityType, store);
            if (!status.available()) {
                statuses.put(store, status);
            }
        }
        return statuses;
    }

    private SchemaPreview schemaPreview(Class<?> entityType,
                                        MapRecord<String, Object, Object> record,
                                        Map<String, String> replayMessage) {
        String fromVersion = replayMessage.getOrDefault(KinexisEvent.EVENT_SCHEMA_VERSION_KEY, KinexisEvent.CURRENT_SCHEMA_VERSION);
        String toVersion = fromVersion;
        boolean requiresUpcast = false;
        String failureReason = "";
        try {
            KinexisEventEnvelope envelope = eventSchemaRegistry.upcast(KinexisEventEnvelope.from(
                    replayMessage,
                    record.getId().getValue(),
                    entityType));
            toVersion = envelope.schemaVersion();
            requiresUpcast = !fromVersion.equals(toVersion);
        } catch (RuntimeException e) {
            toVersion = currentSchemaVersion(entityType);
            requiresUpcast = !fromVersion.equals(toVersion);
            failureReason = "schemaUpcastFailed:" + e.getClass().getName();
        }
        return new SchemaPreview(fromVersion, toVersion, requiresUpcast, failureReason);
    }

    private String currentSchemaVersion(Class<?> entityType) {
        String currentVersion = eventSchemaRegistry.currentVersion(entityType.getName());
        if (currentVersion == null || currentVersion.isBlank()) {
            return KinexisEvent.CURRENT_SCHEMA_VERSION;
        }
        return currentVersion.trim();
    }

    private String skipReason(Map<String, String> replayMessage,
                              List<String> targetStores,
                              List<String> alreadyProcessedStores,
                              Map<String, KinexisStoreHealthStatus> unhealthyStatuses,
                              SchemaPreview schemaPreview,
                              KinexisReplayOptions options) {
        if (!replayMessage.containsKey(EVENT_CONTENT_KEY) || !replayMessage.containsKey(EVENT_OPERATION_KEY)) {
            return "notReplayable";
        }
        if (!schemaPreview.failureReason().isBlank()) {
            return schemaPreview.failureReason();
        }
        if (!targetStores.isEmpty() && alreadyProcessedStores.containsAll(targetStores)) {
            return "alreadyProcessed";
        }
        if ((options == null || !options.force()) && !unhealthyStatuses.isEmpty()) {
            return "unhealthyStore:" + unhealthyStatuses.values().stream()
                    .findFirst()
                    .map(status -> status.state().name())
                    .orElse("");
        }
        return "";
    }

    private List<String> aggregate(List<KinexisReplayPlan.Record> records,
                                   java.util.function.Function<KinexisReplayPlan.Record, List<String>> values) {
        Set<String> aggregate = new LinkedHashSet<>();
        for (KinexisReplayPlan.Record record : records) {
            aggregate.addAll(values.apply(record));
        }
        return List.copyOf(aggregate);
    }

    private Optional<String> replay(Class<?> entityType, String dlqRecordId, ReplayMode replayMode, boolean newEventId, String... targets) {
        String dlqStreamKey = getDLQStreamKey(entityType);
        Optional<MapRecord<String, Object, Object>> record = findRecord(entityType, dlqRecordId);
        if (record.isEmpty()) {
            recordReplayFailure(entityType, null, replayMode, newEventId, "notFound", targets);
            return Optional.empty();
        }
        return replayRecordSafely(dlqStreamKey, entityType, record.get(), replayMode, newEventId, targets);
    }

    private Optional<String> unhealthyReplayReason(Class<?> entityType, String failedStore, KinexisReplayOptions options) {
        if (options != null && options.force()) {
            return Optional.empty();
        }
        if (failedStore == null || failedStore.isBlank()) {
            return Optional.empty();
        }
        KinexisStoreHealthStatus status = storeControl.status(entityType, failedStore);
        if (status.available()) {
            return Optional.empty();
        }
        return Optional.of("unhealthyStore:" + status.state().name());
    }

    private Optional<String> replayRecordSafely(String dlqStreamKey,
                                                Class<?> entityType,
                                                MapRecord<String, Object, Object> record,
                                                ReplayMode replayMode,
                                                boolean newEventId,
                                                String... targets) {
        try {
            return Optional.ofNullable(replayRecord(dlqStreamKey, entityType, record, replayMode, newEventId, targets));
        } catch (RuntimeException e) {
            recordReplayFailure(entityType, record, replayMode, newEventId, e.getClass().getName(), targets);
            throw e;
        }
    }

    private String replayRecord(String dlqStreamKey, Class<?> entityType, MapRecord<String, Object, Object> record,
                                ReplayMode replayMode, boolean newEventId, String... targets) {
        Map<String, String> replayMessage = toReplayMessage(record);
        validateReplayMessage(replayMessage);
        replayMessage = upcastReplayMessage(entityType, record, replayMessage);
        if (newEventId) {
            replayMessage.put(EVENT_ID_KEY, newEventId());
        }
        if (targets != null && targets.length > 0) {
            replayMessage.put(EVENT_TARGETS_KEY, String.join(",", targets));
        }
        Object stream = record.getValue().get(DLQ_STREAM_KEY);
        String targetStream = stream == null ? getStreamKey(entityType) : String.valueOf(stream);
        RecordId replayedId = redisTemplate.opsForStream().add(
                StreamRecords.newRecord()
                        .withId(RecordId.autoGenerate())
                        .ofMap(replayMessage)
                        .withStreamKey(targetStream)
        );
        if (replayedId != null && replayMode == ReplayMode.REPLAY_AND_DELETE) {
            redisTemplate.opsForStream().delete(dlqStreamKey, record.getId());
        }
        if (replayedId == null) {
            recordReplayFailure(entityType, record, replayMode, newEventId, "appendReturnedNull", targets);
            return null;
        }
        telemetry.increment(KinexisTelemetry.DLQ_REPLAYS, replayTags(entityType, targetStream, record, replayMode, newEventId, replayMessage.get(EVENT_TARGETS_KEY)));
        return replayedId.getValue();
    }

    private Map<String, String> upcastReplayMessage(Class<?> entityType,
                                                    MapRecord<String, Object, Object> record,
                                                    Map<String, String> replayMessage) {
        KinexisEventEnvelope envelope = eventSchemaRegistry.upcast(KinexisEventEnvelope.from(
                replayMessage,
                record.getId().getValue(),
                entityType));
        String originalVersion = replayMessage.getOrDefault(KinexisEvent.EVENT_SCHEMA_VERSION_KEY, KinexisEvent.CURRENT_SCHEMA_VERSION);
        if (!originalVersion.equals(envelope.schemaVersion())) {
            telemetry.increment(KinexisTelemetry.STREAM_EVENTS_UPCASTED, Map.of(
                    "entity", entityType.getSimpleName(),
                    "stream", Optional.ofNullable(value(record, DLQ_STREAM_KEY)).orElse(getStreamKey(entityType)),
                    "fromVersion", originalVersion,
                    "toVersion", envelope.schemaVersion()));
        }
        return envelope.toRecordMap();
    }

    private void recordReplayFailure(Class<?> entityType,
                                     MapRecord<String, Object, Object> record,
                                     ReplayMode replayMode,
                                     boolean newEventId,
                                     String reason,
                                     String... targets) {
        String targetStream = record == null || record.getValue().get(DLQ_STREAM_KEY) == null
                ? getStreamKey(entityType)
                : String.valueOf(record.getValue().get(DLQ_STREAM_KEY));
        String targetValue = targets != null && targets.length > 0 ? String.join(",", targets) : recordTargets(record);
        Map<String, String> tags = replayTags(entityType, targetStream, record, replayMode, newEventId, targetValue);
        tags.put("reason", reason == null ? "" : reason);
        telemetry.increment(KinexisTelemetry.DLQ_REPLAY_FAILURES, tags);
    }

    private Map<String, String> replayTags(Class<?> entityType,
                                           String targetStream,
                                           MapRecord<String, Object, Object> record,
                                           ReplayMode replayMode,
                                           boolean newEventId,
                                           String targets) {
        Map<String, String> tags = new HashMap<>();
        tags.put("entity", entityType.getSimpleName());
        tags.put("stream", targetStream == null ? "" : targetStream);
        tags.put("mode", replayMode.name());
        tags.put("failedStore", record == null ? "" : Optional.ofNullable(value(record, DLQ_FAILED_STORE_KEY)).orElse(""));
        tags.put("targets", targets == null ? "" : targets);
        tags.put("eventIdMode", newEventId ? "new" : "preserved");
        return tags;
    }

    private String recordTargets(MapRecord<String, Object, Object> record) {
        if (record == null) {
            return "";
        }
        Object value = record.getValue().get(EVENT_TARGETS_KEY);
        return value == null ? "" : String.valueOf(value);
    }

    private Optional<MapRecord<String, Object, Object>> findRecord(Class<?> entityType, String dlqRecordId) {
        return Optional.ofNullable(redisTemplate.opsForStream().range(getDLQStreamKey(entityType), Range.closed(dlqRecordId, dlqRecordId)))
                .flatMap(records -> records.stream().findFirst());
    }

    private List<MapRecord<String, Object, Object>> records(Class<?> entityType) {
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream()
                .range(getDLQStreamKey(entityType), Range.unbounded());
        return records == null ? List.of() : records;
    }

    private KinexisDlqRecord toDlqRecord(MapRecord<String, Object, Object> record) {
        Map<String, String> values = toStringMap(record);
        return new KinexisDlqRecord(
                record.getId().getValue(),
                values.get(EVENT_ID_KEY),
                values.get(EVENT_ENTITY_TYPE_KEY),
                values.get(EVENT_ENTITY_ID_KEY),
                values.get(EVENT_OPERATION_KEY),
                values.get(DLQ_FAILED_STORE_KEY),
                parseAttempts(values.get(DLQ_ATTEMPTS_KEY)),
                values.get(DLQ_REASON_KEY),
                values.get(DLQ_EXCEPTION_CLASS_KEY),
                parseInstant(values.get(DLQ_FAILURE_TIMESTAMP_KEY)),
                KinexisEvent.targets(values));
    }

    private Map<String, String> toStringMap(MapRecord<String, Object, Object> record) {
        Map<String, String> values = new HashMap<>();
        record.getValue().forEach((key, value) -> values.put(String.valueOf(key), String.valueOf(value)));
        return values;
    }

    private int parseAttempts(String attempts) {
        if (attempts == null || attempts.isBlank()) {
            return 0;
        }
        try {
            return Integer.parseInt(attempts);
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    private boolean olderThan(MapRecord<String, Object, Object> record, Duration age) {
        if (age == null) {
            return true;
        }
        Instant failureTimestamp = parseInstant(value(record, DLQ_FAILURE_TIMESTAMP_KEY));
        return failureTimestamp != null && failureTimestamp.isBefore(Instant.now().minus(age));
    }

    private void delay(Duration delay) {
        if (delay == null || delay.isZero() || delay.isNegative()) {
            return;
        }
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while delaying DLQ batch replay", e);
        }
    }

    private Instant parseInstant(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private String value(MapRecord<String, Object, Object> record, String key) {
        Object value = record.getValue().get(key);
        return value == null ? null : String.valueOf(value);
    }

    private void validateReplayMessage(Map<String, String> replayMessage) {
        if (!replayMessage.containsKey(EVENT_CONTENT_KEY) || !replayMessage.containsKey(EVENT_OPERATION_KEY)) {
            throw new IllegalArgumentException("DLQ record does not contain a replayable Kinexis event");
        }
    }

    private Map<String, String> toReplayMessage(MapRecord<String, Object, Object> record) {
        Map<String, String> replayMessage = toStringMap(record);
        replayMessage.keySet().removeAll(dlqMetadataKeys());
        return replayMessage;
    }

    private java.util.Set<String> dlqMetadataKeys() {
        return java.util.Set.of(
                DLQ_REASON_KEY,
                DLQ_ERROR_KEY,
                DLQ_STREAM_KEY,
                DLQ_STREAM_ID_KEY,
                DLQ_CONSUMER_KEY,
                DLQ_GROUP_KEY,
                DLQ_ATTEMPTS_KEY,
                DLQ_FAILED_STORE_KEY,
                DLQ_EXCEPTION_CLASS_KEY,
                DLQ_FAILURE_TIMESTAMP_KEY
        );
    }

    private record SchemaPreview(String fromVersion, String toVersion, boolean requiresUpcast, String failureReason) {
    }
}
