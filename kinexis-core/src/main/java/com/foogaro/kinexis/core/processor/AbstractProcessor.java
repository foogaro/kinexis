package com.foogaro.kinexis.core.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.foogaro.kinexis.core.Misc;
import com.foogaro.kinexis.core.exception.AcknowledgeMessageException;
import com.foogaro.kinexis.core.exception.ProcessMessageException;
import com.foogaro.kinexis.core.model.KinexisEvent;
import com.foogaro.kinexis.core.store.EntityStore;
import com.foogaro.kinexis.core.store.EntityStoreRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;

import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.foogaro.kinexis.core.Misc.*;

/**
 * Abstract base class for processing Redis Stream messages.
 * This class provides core functionality for handling message processing and acknowledgment
 * in a Redis Stream-based system. It supports CRUD operations on entity stores and manages
 * message acknowledgment through Redis Streams.
 *
 * @param <T> the type of entity that this processor handles
 */
public abstract class AbstractProcessor<T> implements Processor<T> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    @Qualifier("redisTemplate")
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EntityStoreRegistry entityStoreRegistry;

    @Autowired(required = false)
    @Qualifier("kinexisStoreExecutor")
    private Executor storeExecutor = Runnable::run;

    /**
     * Returns the Redis template used for Redis operations.
     *
     * @return the RedisTemplate instance
     */
    @Override
    public RedisTemplate<String, String> getRedisTemplate() {
        return redisTemplate;
    }

    /**
     * Returns the ObjectMapper used for JSON serialization/deserialization.
     *
     * @return the ObjectMapper instance
     */
    @Override
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    private final Class<T> entityClass;

    /**
     * No-args constructor for AbstractProcessor.
     * Initializes the entity class using reflection.
     */
    @SuppressWarnings("unchecked")
    protected AbstractProcessor() {
        this.entityClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     * Converts a JSON string to an entity object.
     *
     * @param content the JSON string to convert
     * @return the converted entity object
     * @throws JsonProcessingException if the JSON string cannot be processed
     */
    public T convertToEntity(String content) throws JsonProcessingException {
        return getObjectMapper().readValue(content, getEntityClass());
    }

    /**
     * Returns the class of the entity being processed.
     *
     * @return the entity class
     */
    public Class<T> getEntityClass() {
        return entityClass;
    }

    /**
     * Processes a Redis Stream record by performing the appropriate CRUD operation
     * on all relevant entity stores. Supports both save and delete operations.
     *
     * @param record the Redis Stream record to process
     * @throws ProcessMessageException if an error occurs during processing
     */
    public void process(final MapRecord<String, String, String> record) throws ProcessMessageException {
        List<String> targets = KinexisEvent.targets(record.getValue());
        List<EntityStore<T>> stores = entityStoreRegistry.findTargetStores(getEntityClass(), targets);
        if (stores.isEmpty()) {
            throw new ProcessMessageException(new IllegalStateException("No target store found for entity " + getEntityClass().getSimpleName() + " and targets " + targets));
        }

        try {
            String content = record.getValue().get(EVENT_CONTENT_KEY);
            String operation = record.getValue().get(EVENT_OPERATION_KEY);
            logger.debug("Processing message: {}", record.getId());
            logger.debug("Processing EVENT_CONTENT_KEY: {}", content);
            logger.debug("Processing EVENT_OPERATION_KEY: {}", operation);
            logger.debug("Processing {}: {}", KinexisEvent.EVENT_TARGETS_KEY, targets);
            if (Misc.Operation.DELETE.getValue().equals(operation)) {
                fanOut(record, stores, store -> {
                    logger.trace("Deleting message {} in store {}", record.getId(), store.name());
                    store.deleteById(content);
                    logger.trace("Deleted message {} in store {}", record.getId(), store.name());
                }, "delete");
            } else {
                T entity = convertToEntity(content);
                fanOut(record, stores, store -> {
                    logger.trace("Saving message {} in store {}", record.getId(), store.name());
                    store.save(entity);
                    logger.trace("Saved message {} in store {}", record.getId(), store.name());
                }, "save");
            }
            logger.info("Processed message: {}", record.getId());
        } catch (ProcessMessageException e) {
            logger.error("Error processing message: {}\n{}", record.getId(), e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("Error processing message: {}\n{}", record.getId(), e.getMessage());
            throw new ProcessMessageException(e);
        }
    }

    private void fanOut(MapRecord<String, String, String> record,
                        List<EntityStore<T>> stores,
                        StoreOperation<T> operation,
                        String operationName) throws ProcessMessageException {
        List<CompletableFuture<StoreFailure>> futures = stores.stream()
                .map(store -> CompletableFuture.supplyAsync(() -> executeStoreOperation(store, operation), storeExecutor))
                .toList();
        List<StoreFailure> failures = futures.stream()
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .toList();
        if (!failures.isEmpty()) {
            List<String> failedStores = failures.stream().map(StoreFailure::storeName).toList();
            Throwable cause = failures.getFirst().cause();
            throw new ProcessMessageException(
                    "Stores " + failedStores + " failed to " + operationName + " message " + record.getId(),
                    cause,
                    failedStores);
        }
    }

    private StoreFailure executeStoreOperation(EntityStore<T> store, StoreOperation<T> operation) {
        try {
            operation.apply(store);
            return null;
        } catch (Exception e) {
            return new StoreFailure(store.name(), e);
        }
    }

    @FunctionalInterface
    private interface StoreOperation<T> {
        void apply(EntityStore<T> store);
    }

    private record StoreFailure(String storeName, Throwable cause) {
    }

    /**
     * Acknowledges a processed Redis Stream record.
     * This marks the message as successfully processed in the Redis Stream.
     *
     * @param record the Redis Stream record to acknowledge
     * @throws AcknowledgeMessageException if an error occurs during acknowledgment
     */
    public void acknowledge(final MapRecord<String, String, String> record) throws AcknowledgeMessageException {
        try {
            logger.debug("Acknowledging message: {}", record.getId());
            getRedisTemplate().opsForStream().acknowledge(getConsumerGroup(entityClass), record);
            logger.debug("Acknowledged message: {} for group: {}", record.getId(), getConsumerGroup(entityClass));
        } catch (Exception e) {
            logger.error("Error acknowledging message: {}\n{}", record.getId(), e.getMessage());
            throw new AcknowledgeMessageException(e);
        }
    }

}
