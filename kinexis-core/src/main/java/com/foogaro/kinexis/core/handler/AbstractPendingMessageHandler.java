package com.foogaro.kinexis.core.handler;

import com.foogaro.kinexis.core.exception.AcknowledgeMessageException;
import com.foogaro.kinexis.core.exception.ProcessMessageException;
import com.foogaro.kinexis.core.processor.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
 * @param <R> the type of repository used for entity operations
 */
public abstract class AbstractPendingMessageHandler<T, R> implements MessageHandler<T, R> {

    private Logger logger = LoggerFactory.getLogger(getClass());

    /** Maximum number of processing attempts for a message */
    @Value("${kinexis.stream.listener.pel.max-attempts:3}")
    protected int MAX_ATTEMPTS;
    /** Maximum retention time for message processing attempts in milliseconds */
    @Value("${kinexis.stream.listener.pel.max-retention:120000}")
    protected long MAX_RETENTION;
    /** Number of messages to process in each batch */
    @Value("${kinexis.stream.listener.pel.batch-size:50}")
    protected int BATCH_SIZE;
    /** Fixed delay between processing attempts in milliseconds */
    @Value("${kinexis.stream.listener.pel.fixed-delay:300000}")
    protected final long fixedDelay = 300000;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    private final Class<T> entityClass;
    private final Class<R> repositoryClass;

    /**
     * Gets the processor that handles the actual message processing logic.
     * This method must be implemented by concrete subclasses.
     *
     * @return the Processor instance that handles message processing
     */
    public abstract Processor<T, R> getProcessor();

    /**
     * Constructs a new AbstractPendingMessageHandler and initializes the entity
     * and repository classes using reflection to determine the generic type parameters.
     *
     * @throws ClassCastException if the generic type parameters cannot be determined
     */
    @SuppressWarnings("unchecked")
    public AbstractPendingMessageHandler() {
        super();
        this.entityClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        this.repositoryClass = (Class<R>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1];
    }

    /**
     * Gets the Redis template used for Redis operations.
     *
     * @return the RedisTemplate instance
     */
    public RedisTemplate<String, String> getRedisTemplate() {
        return redisTemplate;
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
     * Gets the repository class that manages the entity.
     *
     * @return the repository class
     */
    public Class<R> getRepositoryClass() {
        return repositoryClass;
    }

    /**
     * Processes pending messages from the Redis Stream.
     * This scheduled method:
     * 1. Retrieves pending messages for the consumer group
     * 2. Processes each message up to the maximum number of attempts
     * 3. Handles message failures by moving them to a dead letter queue
     * 4. Manages message acknowledgment and retry counters
     */
    @Scheduled(fixedDelay = fixedDelay)
    public void processPendingMessages() {
        String streamKey = getStreamKey(entityClass);
        String groupName = getConsumerGroup(repositoryClass);
        String consumerName = getConsumerName(entityClass, repositoryClass);

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
                            Long counter = incrementCounterKey(getCounterKey(message.getId().getValue()));
                            logger.debug("Attempts: {} - Elapsed time: {}", counter, elapsedTime);
                            try {
                                getProcessor().process(message);
                                getProcessor().acknowledge(message);
                                expireCounterKey(getCounterKey(message.getId().getValue()));
                                logger.info("Successfully processed pending message: {}", messageId);
                            } catch (ProcessMessageException e) {
                                logger.error("Error processing pending message: {} - {}", messageId, e.getMessage());
                                if (counter >= MAX_ATTEMPTS) {
                                    handleMessageFailure(message, "Too many attempts", new RuntimeException(e), getCounterKey(message.getId().getValue()));
                                    throw new RuntimeException(e);
                                }
                            } catch (AcknowledgeMessageException e) {
                                if (counter >= MAX_ATTEMPTS) {
                                    handleMessageFailure(message, "Long lasting message", new RuntimeException(e), getCounterKey(message.getId().getValue()));
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
                                      String counterKey) {
        handleDLQ(message, dlqReason, cause);
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
    private String getCounterKey(String id) {
        return getStreamKey(entityClass) + KEY_SEPARATOR + id;
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
    private void handleDLQ(MapRecord<String, String, String> message, String dlqReason, Exception e) {
        try {
            if (message != null) {
                dumpMessage(message);
                logger.error("Received error: {}", e.getMessage());
                String deadLetterKey = getDLQStreamKey(entityClass);
                Map<String, String> deadLetterMessage = new HashMap<>(message.getValue());
                deadLetterMessage.put("reason", dlqReason);
                deadLetterMessage.put("error", e.getMessage());
                deadLetterMessage.put("streamKey", message.getStream());
                deadLetterMessage.put("streamID", message.getId().getValue());
                deadLetterMessage.put("consumer", getConsumerName(entityClass, repositoryClass));
                deadLetterMessage.put("group", getConsumerGroup(repositoryClass));

                redisTemplate.opsForStream().add(
                        StreamRecords.newRecord()
                                .withId(RecordId.autoGenerate())
                                .ofMap(deadLetterMessage)
                                .withStreamKey(deadLetterKey)
                );
                logger.warn("Message {} moved to dead letter queue for manual processing.", message.getId());
                redisTemplate.opsForStream().acknowledge(getConsumerGroup(repositoryClass), message);
                logger.warn("And Message {} acknowledged.", message.getId());
            }
        } catch (Exception dlqError) {
            logger.error("Error while moving message to dead letter queue: {}", dlqError.getMessage());
        }
    }
}
