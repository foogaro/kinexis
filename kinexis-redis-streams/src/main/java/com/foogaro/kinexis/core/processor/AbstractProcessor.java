package com.foogaro.kinexis.core.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.foogaro.kinexis.core.Misc;
import com.foogaro.kinexis.core.config.KinexisProperties;
import com.foogaro.kinexis.core.exception.AcknowledgeMessageException;
import com.foogaro.kinexis.core.exception.KinexisBackpressureException;
import com.foogaro.kinexis.core.exception.ProcessMessageException;
import com.foogaro.kinexis.core.model.KinexisEvent;
import com.foogaro.kinexis.core.store.EntityStore;
import com.foogaro.kinexis.core.store.EntityStoreRegistry;
import com.foogaro.kinexis.core.telemetry.KinexisTelemetry;
import com.foogaro.kinexis.core.telemetry.SimpleKinexisTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;

import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    @Autowired(required = false)
    private KinexisProcessingCoordinator processingCoordinator;

    @Autowired(required = false)
    private KinexisProcessingMetrics processingMetrics;

    @Autowired(required = false)
    private KinexisTelemetry telemetry;

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
        try {
            ProcessingContext<T> context = processingContext(record);
            coordinator().inStreamCapacity(record.getStream(), () -> {
                coordinator().inEntityOrder(getEntityClass(), context.entityId(), () -> {
                    processInEntityOrder(record, context);
                    return null;
                });
                return null;
            });
        } catch (ProcessMessageException e) {
            logger.error("Error processing message: {}\n{}", record.getId(), e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("Error processing message: {}\n{}", record.getId(), e.getMessage());
            throw new ProcessMessageException(e);
        }
    }

    private void processInEntityOrder(MapRecord<String, String, String> record,
                                      ProcessingContext<T> context) throws ProcessMessageException {
        List<EntityStore<T>> stores = entityStoreRegistry.findTargetStores(getEntityClass(), context.targets());
        if (stores.isEmpty()) {
            throw new ProcessMessageException(new IllegalStateException("No target store found for entity " + getEntityClass().getSimpleName() + " and targets " + context.targets()));
        }

        logger.debug("Processing message: {}", record.getId());
        logger.debug("Processing {}: {}", KinexisEvent.EVENT_ID_KEY, context.eventId());
        logger.debug("Processing {}: {}", KinexisEvent.EVENT_ENTITY_ID_KEY, context.entityId());
        logger.debug("Processing EVENT_CONTENT_KEY: {}", context.content());
        logger.debug("Processing EVENT_OPERATION_KEY: {}", context.operation());
        logger.debug("Processing {}: {}", KinexisEvent.EVENT_TARGETS_KEY, context.targets());
        if (Misc.Operation.DELETE.getValue().equals(context.operation())) {
            fanOut(record, context, stores, store -> {
                logger.trace("Deleting message {} in store {}", record.getId(), store.name());
                store.deleteById(context.content());
                logger.trace("Deleted message {} in store {}", record.getId(), store.name());
            }, "delete");
        } else {
            fanOut(record, context, stores, store -> {
                logger.trace("Saving message {} in store {}", record.getId(), store.name());
                store.save(context.entity());
                logger.trace("Saved message {} in store {}", record.getId(), store.name());
            }, "save");
        }
        telemetry().increment(KinexisTelemetry.STREAM_EVENTS_PROCESSED, Map.of(
                "entity", getEntityClass().getSimpleName(),
                "operation", context.operation(),
                "stream", record.getStream()));
        logger.info("Processed message: {}", record.getId());
    }

    private ProcessingContext<T> processingContext(MapRecord<String, String, String> record) throws JsonProcessingException {
        String content = record.getValue().get(EVENT_CONTENT_KEY);
        String operation = record.getValue().get(EVENT_OPERATION_KEY);
        String eventId = KinexisEvent.eventId(record.getValue(), record.getId().getValue());
        List<String> targets = KinexisEvent.targets(record.getValue());
        if (Misc.Operation.DELETE.getValue().equals(operation)) {
            String entityId = KinexisEvent.entityId(record.getValue()).orElse(content);
            return new ProcessingContext<>(eventId, entityId, operation, content, targets, null);
        }
        T entity = convertToEntity(content);
        String entityId = KinexisEvent.entityId(record.getValue())
                .or(() -> Misc.getEntityId(entity).map(String::valueOf))
                .orElse(null);
        return new ProcessingContext<>(eventId, entityId, operation, content, targets, entity);
    }

    private void fanOut(MapRecord<String, String, String> record,
                        ProcessingContext<T> context,
                        List<EntityStore<T>> stores,
                        StoreOperation<T> operation,
                        String operationName) throws ProcessMessageException {
        List<CompletableFuture<StoreFailure>> futures = stores.stream()
                .map(store -> submitStoreOperation(record, context, store, operation))
                .toList();
        List<StoreFailure> failures = futures.stream()
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .toList();
        if (!failures.isEmpty()) {
            List<String> failedStores = failedTargets(failures);
            Throwable cause = failures.getFirst().cause();
            throw new ProcessMessageException(
                    "Stores " + failedStores + " failed to " + operationName + " message " + record.getId(),
                    cause,
                    failedStores);
        }
    }

    private CompletableFuture<StoreFailure> submitStoreOperation(MapRecord<String, String, String> record,
                                                                 ProcessingContext<T> context,
                                                                 EntityStore<T> store,
                                                                 StoreOperation<T> operation) {
        try {
            return CompletableFuture.supplyAsync(() -> executeStoreOperation(record, context, store, operation), storeExecutor);
        } catch (RejectedExecutionException e) {
            boolean kinexisRejection = e.getCause() instanceof KinexisBackpressureException;
            if (!kinexisRejection) {
                metrics().recordBackpressureRejection();
            }
            Throwable cause = kinexisRejection ? e.getCause() : new KinexisBackpressureException("Kinexis store executor rejected work", e);
            return CompletableFuture.completedFuture(new StoreFailure(store.name(), matchingTargets(context, store), cause));
        }
    }

    private StoreFailure executeStoreOperation(MapRecord<String, String, String> record,
                                               ProcessingContext<T> context,
                                               EntityStore<T> store,
                                               StoreOperation<T> operation) {
        long startedAt = System.nanoTime();
        try {
            if (coordinator().isProcessed(getEntityClass(), context.eventId(), store.name())) {
                logger.debug("Skipping already processed event {} for store {} and message {}", context.eventId(), store.name(), record.getId());
                return null;
            }
            operation.apply(store);
            coordinator().markProcessed(getEntityClass(), context.eventId(), store.name());
            return null;
        } catch (Exception e) {
            metrics().recordStoreTaskFailed();
            telemetry().increment(KinexisTelemetry.STORE_FAILURES, Map.of(
                    "entity", getEntityClass().getSimpleName(),
                    "store", store.name(),
                    "operation", context.operation(),
                    "exception", e.getClass().getName()));
            return new StoreFailure(store.name(), matchingTargets(context, store), e);
        } finally {
            telemetry().recordDuration(KinexisTelemetry.STORE_WRITE_LATENCY,
                    java.time.Duration.ofNanos(System.nanoTime() - startedAt),
                    Map.of(
                            "entity", getEntityClass().getSimpleName(),
                            "store", store.name(),
                            "operation", context.operation()));
        }
    }

    private List<String> matchingTargets(ProcessingContext<T> context, EntityStore<T> store) {
        if (context.targets().isEmpty()) {
            return List.of();
        }
        return context.targets().stream()
                .filter(store.targets()::contains)
                .distinct()
                .toList();
    }

    private List<String> failedTargets(List<StoreFailure> failures) {
        Map<String, Long> targetCounts = failures.stream()
                .flatMap(failure -> failure.matchingTargets().stream())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        return failures.stream()
                .map(failure -> failure.matchingTargets().stream()
                        .filter(target -> targetCounts.getOrDefault(target, 0L) == 1L)
                        .findFirst()
                        .orElse(failure.storeName()))
                .toList();
    }

    @FunctionalInterface
    private interface StoreOperation<T> {
        void apply(EntityStore<T> store);
    }

    private record StoreFailure(String storeName, List<String> matchingTargets, Throwable cause) {
    }

    private record ProcessingContext<T>(String eventId,
                                        String entityId,
                                        String operation,
                                        String content,
                                        List<String> targets,
                                        T entity) {
    }

    private KinexisProcessingCoordinator coordinator() {
        if (processingCoordinator == null) {
            processingCoordinator = new KinexisProcessingCoordinator(getRedisTemplate(), new KinexisProperties(), metrics());
        }
        return processingCoordinator;
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
