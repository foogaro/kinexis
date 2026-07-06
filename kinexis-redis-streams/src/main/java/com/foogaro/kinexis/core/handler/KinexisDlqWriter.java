package com.foogaro.kinexis.core.handler;

import com.foogaro.kinexis.core.exception.ProcessMessageException;
import com.foogaro.kinexis.core.processor.KinexisProcessingMetrics;
import com.foogaro.kinexis.core.telemetry.KinexisTelemetry;
import com.foogaro.kinexis.core.telemetry.SimpleKinexisTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.foogaro.kinexis.core.Misc.dumpMessage;
import static com.foogaro.kinexis.core.Misc.getDLQStreamKey;
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

public class KinexisDlqWriter {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final RedisTemplate<String, String> redisTemplate;
    private final KinexisProcessingMetrics metrics;
    private final KinexisTelemetry telemetry;

    public KinexisDlqWriter(RedisTemplate<String, String> redisTemplate, KinexisProcessingMetrics metrics) {
        this(redisTemplate, metrics, new SimpleKinexisTelemetry());
    }

    public KinexisDlqWriter(RedisTemplate<String, String> redisTemplate,
                            KinexisProcessingMetrics metrics,
                            KinexisTelemetry telemetry) {
        this.redisTemplate = redisTemplate;
        this.metrics = metrics;
        this.telemetry = telemetry;
    }

    public void moveToDlq(Class<?> entityClass,
                          MapRecord<String, String, String> message,
                          String reason,
                          Exception exception,
                          long attempts,
                          String groupName,
                          String consumerName) {
        if (message == null) {
            return;
        }
        dumpMessage(message);
        logger.error("Received error: {}", exception.getMessage());
        String deadLetterKey = getDLQStreamKey(entityClass);
        List<String> failedStores = failedStores(exception);
        if (failedStores.isEmpty()) {
            writeDeadLetterRecord(entityClass, deadLetterKey, message, reason, exception, attempts, groupName, consumerName, null);
        } else {
            for (String failedStore : failedStores) {
                writeDeadLetterRecord(entityClass, deadLetterKey, message, reason, exception, attempts, groupName, consumerName, failedStore);
            }
        }

        redisTemplate.opsForStream().acknowledge(groupName, message);
        logger.warn("Message {} moved to dead letter queue for manual processing.", message.getId());
        logger.warn("And Message {} acknowledged.", message.getId());
    }

    private void writeDeadLetterRecord(Class<?> entityClass,
                                       String deadLetterKey,
                                       MapRecord<String, String, String> message,
                                       String reason,
                                       Exception exception,
                                       long attempts,
                                       String groupName,
                                       String consumerName,
                                       String failedStore) {
        Map<String, String> deadLetterMessage = new HashMap<>(message.getValue());
        deadLetterMessage.put(DLQ_REASON_KEY, reason);
        deadLetterMessage.put(DLQ_ERROR_KEY, exception.getMessage());
        deadLetterMessage.put(DLQ_STREAM_KEY, message.getStream());
        deadLetterMessage.put(DLQ_STREAM_ID_KEY, message.getId().getValue());
        deadLetterMessage.put(DLQ_CONSUMER_KEY, consumerName);
        deadLetterMessage.put(DLQ_GROUP_KEY, groupName);
        deadLetterMessage.put(DLQ_ATTEMPTS_KEY, String.valueOf(attempts));
        deadLetterMessage.put(DLQ_EXCEPTION_CLASS_KEY, exception.getClass().getName());
        deadLetterMessage.put(DLQ_FAILURE_TIMESTAMP_KEY, Instant.now().toString());
        if (failedStore != null) {
            deadLetterMessage.put(DLQ_FAILED_STORE_KEY, failedStore);
            deadLetterMessage.put(EVENT_TARGETS_KEY, failedStore);
        }

        redisTemplate.opsForStream().add(
                StreamRecords.newRecord()
                        .withId(RecordId.autoGenerate())
                        .ofMap(deadLetterMessage)
                        .withStreamKey(deadLetterKey)
        );
        metrics.recordDeadLetteredRecord();
        telemetry.increment(KinexisTelemetry.DLQ_RECORDS, Map.of(
                "entity", entityClass.getSimpleName(),
                "stream", message.getStream(),
                "reason", reason,
                "failedStore", failedStore == null ? "" : failedStore));
    }

    private List<String> failedStores(Exception exception) {
        if (exception instanceof ProcessMessageException processMessageException) {
            return processMessageException.getFailedStores();
        }
        return List.of();
    }
}
