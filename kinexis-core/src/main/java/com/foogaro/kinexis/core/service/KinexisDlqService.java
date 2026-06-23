package com.foogaro.kinexis.core.service;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.foogaro.kinexis.core.Misc.EVENT_CONTENT_KEY;
import static com.foogaro.kinexis.core.Misc.EVENT_OPERATION_KEY;
import static com.foogaro.kinexis.core.Misc.getDLQStreamKey;
import static com.foogaro.kinexis.core.Misc.getStreamKey;
import static com.foogaro.kinexis.core.model.KinexisEvent.EVENT_TARGETS_KEY;
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

    public KinexisDlqService(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public Optional<String> replay(Class<?> entityType, String dlqRecordId) {
        return replay(entityType, dlqRecordId, ReplayMode.REPLAY_ONLY);
    }

    public Optional<String> replay(Class<?> entityType, String dlqRecordId, String... targets) {
        return replay(entityType, dlqRecordId, ReplayMode.REPLAY_ONLY, targets);
    }

    public Optional<String> replay(Class<?> entityType, String dlqRecordId, ReplayMode replayMode, String... targets) {
        String dlqStreamKey = getDLQStreamKey(entityType);
        return Optional.ofNullable(redisTemplate.opsForStream().range(dlqStreamKey, Range.closed(dlqRecordId, dlqRecordId)))
                .flatMap(records -> records.stream().findFirst())
                .map(record -> replayRecord(dlqStreamKey, entityType, record, replayMode, targets));
    }

    private String replayRecord(String dlqStreamKey, Class<?> entityType, MapRecord<String, Object, Object> record,
                                ReplayMode replayMode, String... targets) {
        Map<String, String> replayMessage = toReplayMessage(record);
        validateReplayMessage(replayMessage);
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
        return replayedId == null ? null : replayedId.getValue();
    }

    private void validateReplayMessage(Map<String, String> replayMessage) {
        if (!replayMessage.containsKey(EVENT_CONTENT_KEY) || !replayMessage.containsKey(EVENT_OPERATION_KEY)) {
            throw new IllegalArgumentException("DLQ record does not contain a replayable Kinexis event");
        }
    }

    private Map<String, String> toReplayMessage(MapRecord<String, Object, Object> record) {
        Map<String, String> replayMessage = new HashMap<>();
        record.getValue().forEach((key, value) -> replayMessage.put(String.valueOf(key), String.valueOf(value)));
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
}
