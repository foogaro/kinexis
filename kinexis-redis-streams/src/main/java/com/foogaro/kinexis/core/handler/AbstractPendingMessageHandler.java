package com.foogaro.kinexis.core.handler;

import com.foogaro.kinexis.core.config.KinexisProperties;
import com.foogaro.kinexis.core.exception.AcknowledgeMessageException;
import com.foogaro.kinexis.core.exception.KinexisBackpressureException;
import com.foogaro.kinexis.core.exception.ProcessMessageException;
import com.foogaro.kinexis.core.processor.KinexisProcessingMetrics;
import com.foogaro.kinexis.core.processor.Processor;
import com.foogaro.kinexis.core.stream.StreamPartitioner;
import com.foogaro.kinexis.core.telemetry.KinexisTelemetry;
import com.foogaro.kinexis.core.telemetry.SimpleKinexisTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.lang.reflect.ParameterizedType;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static com.foogaro.kinexis.core.Misc.*;

/**
 * Abstract base class for handling pending messages in Redis Streams.
 * This class provides functionality for processing messages that have been
 * delivered but not acknowledged, implementing retry logic and dead letter queue
 * handling for failed messages.
 *
 * @param <T> the type of entity that this handler processes
 */
public abstract class AbstractPendingMessageHandler<T> {

    public static final String DLQ_REASON_KEY = "reason";
    public static final String DLQ_ERROR_KEY = "error";
    public static final String DLQ_STREAM_KEY = "streamKey";
    public static final String DLQ_STREAM_ID_KEY = "streamID";
    public static final String DLQ_CONSUMER_KEY = "consumer";
    public static final String DLQ_GROUP_KEY = "group";
    public static final String DLQ_ATTEMPTS_KEY = "attempts";
    public static final String DLQ_FAILED_STORE_KEY = "failedStore";
    public static final String DLQ_EXCEPTION_CLASS_KEY = "exceptionClass";
    public static final String DLQ_FAILURE_TIMESTAMP_KEY = "failureTimestamp";

    private Logger logger = LoggerFactory.getLogger(getClass());

    protected int MAX_ATTEMPTS = 3;
    protected long MAX_RETENTION = 120000;
    protected int BATCH_SIZE = 50;
    private KinexisProperties properties = new KinexisProperties();

    @Autowired
    @Qualifier("redisTemplate")
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private StreamPartitioner streamPartitioner;

    @Autowired(required = false)
    private KinexisDlqWriter dlqWriter;

    @Autowired(required = false)
    private KinexisProcessingMetrics processingMetrics;

    @Autowired(required = false)
    private KinexisTelemetry telemetry;

    private final Class<T> entityClass;

    @Autowired
    public void setKinexisProperties(KinexisProperties properties) {
        this.properties = properties;
        KinexisProperties.Pending pending = properties.getStream().getListener().getPending();
        MAX_ATTEMPTS = pending.getMaxAttempts();
        MAX_RETENTION = pending.getMaxRetention();
        BATCH_SIZE = pending.getBatchSize();
    }

    /**
     * Gets the processor that handles the actual message processing logic.
     * This method must be implemented by concrete subclasses.
     *
     * @return the Processor instance that handles message processing
     */
    public abstract Processor<T> getProcessor();

    /**
     * Constructs a new AbstractPendingMessageHandler and initializes the entity class
     * using reflection to determine the generic type parameter.
     *
     * @throws ClassCastException if the generic type parameters cannot be determined
     */
    @SuppressWarnings("unchecked")
    public AbstractPendingMessageHandler() {
        super();
        this.entityClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * Gets the entity class that this handler processes.
     *
     * @return the entity class
     */
    public Class<T> getEntityClass() {
        return this.entityClass;
    }

    /**
     * Processes pending messages from the Redis Stream.
     * This scheduled method:
     * 1. Retrieves pending messages for the consumer group
     * 2. Processes each message up to the maximum number of attempts
     * 3. Handles message failures by moving them to a dead letter queue
     * 4. Manages message acknowledgment and retry counters
     */
    @Scheduled(fixedDelayString = "${kinexis.stream.listener.pending.fixed-delay:300000}")
    public void processPendingMessages() {
        String groupName = getConsumerGroup(entityClass);
        String consumerName = getConsumerName(entityClass);

        for (String streamKey : streamPartitioner().streamKeys(entityClass)) {
            processPendingMessages(streamKey, groupName, consumerName);
        }
    }

    private StreamPartitioner streamPartitioner() {
        if (streamPartitioner == null) {
            streamPartitioner = new StreamPartitioner(new KinexisProperties());
        }
        return streamPartitioner;
    }

    private void processPendingMessages(String streamKey, String groupName, String consumerName) {
        try {
            PendingMessagesSummary pendingSummary = redisTemplate.opsForStream()
                    .pending(streamKey, groupName);

            if (pendingSummary != null && pendingSummary.getTotalPendingMessages() > 0) {
                logger.info("Found {} pending messages for group {}",
                        pendingSummary.getTotalPendingMessages(), groupName);

                PendingMessages pendingMessages = redisTemplate.opsForStream()
                        .pending(streamKey,
                                Consumer.from(groupName, consumerName),
                                Range.unbounded(),
                                BATCH_SIZE);

                if (pendingMessages != null) {
                    for (PendingMessage pm : pendingMessages) {
                        String messageId = pm.getIdAsString();
                        long elapsedTime = pm.getElapsedTimeSinceLastDelivery().toMillis();
                        logger.info("Message ID {} re-processing", messageId);

                        MapRecord<String, String, String> message = null;
                            List<MapRecord<String, Object, Object>> rawMessages =
                                    redisTemplate.opsForStream().range(streamKey,
                                            Range.closed(messageId, messageId));
                        List<MapRecord<String, String, String>> messages =
                                (rawMessages != null) ? rawMessages
                                    .stream()
                                    .map(this::convertMapRecord)
                                    .collect(Collectors.toList())
                                : Collections.emptyList();

                        if (!messages.isEmpty()) {
                            message = messages.get(0);
                            String counterKey = getCounterKey(message.getStream(), message.getId().getValue());
                            Long counter = incrementCounterKey(counterKey);
                            metrics().recordPendingRetry();
                            telemetry().increment(KinexisTelemetry.PENDING_RETRIES, Map.of(
                                    "entity", entityClass.getSimpleName(),
                                    "stream", message.getStream(),
                                    "group", groupName));
                            logger.debug("Attempts: {} - Elapsed time: {}", counter, elapsedTime);
                            try {
                                getProcessor().process(message);
                                getProcessor().acknowledge(message);
                                expireCounterKey(counterKey);
                                logger.info("Successfully processed pending message: {}", messageId);
                            } catch (ProcessMessageException e) {
                                logger.error("Error processing pending message: {} - {}", messageId, e.getMessage());
                                if (shouldMoveToDlqImmediately(e) || counter >= MAX_ATTEMPTS) {
                                    handleMessageFailure(message, dlqReason(e), e, counter, counterKey);
                                    throw new RuntimeException(e);
                                }
                            } catch (AcknowledgeMessageException e) {
                                if (counter >= MAX_ATTEMPTS) {
                                    handleMessageFailure(message, "Long lasting message", e, counter, counterKey);
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    }
                }
            } else {
                logger.debug("Pending messages not found for group {}", groupName);
            }
        } catch (Exception e) {
            logger.error("Error processing pending messages: {}", e.getMessage());
        }
    }

    /**
     * Converts a MapRecord with Object values to a MapRecord with String values.
     *
     * @param record the original MapRecord to convert
     * @return the converted MapRecord with String values
     * @throws NullPointerException if record is null
     */
    private MapRecord<String, String, String> convertMapRecord(MapRecord<String, Object, Object> record) {
        Objects.requireNonNull(record, "Record cannot be null");
        Map<String, String> convertedMap = new HashMap<>();
        record.getValue().forEach((k, v) -> {
            convertedMap.put(String.valueOf(k),String.valueOf(v));
        });

        return StreamRecords.newRecord()
                .withId(record.getId())
                .ofMap(convertedMap)
                .withStreamKey(record.getStream());
    }

    /**
     * Handles message processing failure by moving the message to the dead letter queue
     * and cleaning up the retry counter.
     *
     * @param message the failed message
     * @param dlqReason the reason for moving to dead letter queue
     * @param cause the exception that caused the failure
     * @param counterKey the key for the retry counter
     */
    private void handleMessageFailure(MapRecord<String, String, String> message,
                                      String dlqReason, Exception cause,
                                      long attempts,
                                      String counterKey) {
        handleDLQ(message, dlqReason, cause, attempts);
        expireCounterKey(counterKey);
    }

    /**
     * Expires the retry counter key for a message.
     *
     * @param counterKey the key to expire
     * @throws NullPointerException if counterKey is null
     */
    private void expireCounterKey(String counterKey) {
        Objects.requireNonNull(counterKey, "Counter key cannot be null");
        redisTemplate.expire(counterKey, Duration.ZERO);
    }

    /**
     * Increments the retry counter for a message and sets its expiration time.
     *
     * @param counterKey the key for the retry counter
     * @return the new counter value, or 0 if the increment failed
     * @throws NullPointerException if counterKey is null
     */
    private Long incrementCounterKey(String counterKey) {
        Objects.requireNonNull(counterKey, "Counter key cannot be null");
        Long incr =redisTemplate.opsForValue().increment(counterKey);
        if (incr != null) {
            redisTemplate.expire(counterKey, Duration.ofMillis(MAX_RETENTION));
        } else return 0L;
        return incr;
    }

    /**
     * Generates the counter key for a message ID.
     *
     * @param id the message ID
     * @return the counter key
     */
    private String getCounterKey(String streamKey, String id) {
        return streamKey + KEY_SEPARATOR + id;
    }

    /**
     * Handles moving a failed message to the dead letter queue.
     * This method:
     * 1. Dumps the message for debugging
     * 2. Creates a dead letter message with additional failure information
     * 3. Adds the message to the dead letter queue
     * 4. Acknowledges the original message
     *
     * @param message the failed message
     * @param dlqReason the reason for moving to dead letter queue
     * @param e the exception that caused the failure
     */
    private void handleDLQ(MapRecord<String, String, String> message, String dlqReason, Exception e, long attempts) {
        try {
            dlqWriter().moveToDlq(entityClass, message, dlqReason, e, attempts, getConsumerGroup(entityClass), getConsumerName(entityClass));
        } catch (Exception dlqError) {
            logger.error("Error while moving message to dead letter queue: {}", dlqError.getMessage());
        }
    }

    private boolean shouldMoveToDlqImmediately(ProcessMessageException exception) {
        return properties.getProcessing().getBackpressure().getQueueFullBehavior() == KinexisProperties.QueueFullBehavior.REJECT_TO_DLQ
                && hasBackpressureCause(exception);
    }

    private String dlqReason(ProcessMessageException exception) {
        return shouldMoveToDlqImmediately(exception) ? "Backpressure rejected" : "Too many attempts";
    }

    private boolean hasBackpressureCause(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof KinexisBackpressureException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private KinexisDlqWriter dlqWriter() {
        if (dlqWriter == null) {
            dlqWriter = new KinexisDlqWriter(redisTemplate, metrics());
        }
        return dlqWriter;
    }

    private KinexisProcessingMetrics metrics() {
        if (processingMetrics == null) {
            processingMetrics = new KinexisProcessingMetrics();
        }
        return processingMetrics;
    }

    private KinexisTelemetry telemetry() {
        if (telemetry == null) {
            telemetry = new SimpleKinexisTelemetry();
        }
        return telemetry;
    }
}
