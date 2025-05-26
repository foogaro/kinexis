package com.foogaro.kinexis.core.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.foogaro.kinexis.core.Misc;
import com.foogaro.kinexis.core.exception.AcknowledgeMessageException;
import com.foogaro.kinexis.core.exception.ProcessMessageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.Repository;

import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.foogaro.kinexis.core.Misc.*;

/**
 * Abstract base class for processing Redis Stream messages.
 * This class provides core functionality for handling message processing and acknowledgment
 * in a Redis Stream-based system. It supports CRUD operations on entities and manages
 * message acknowledgment through Redis Streams.
 *
 * @param <T> the type of entity that this processor handles
 * @param <R> the type of repository used for entity operations
 */
public abstract class AbstractProcessor<T, R> implements Processor<T, R> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

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

    /** The Redis Stream record to be processed */
    protected final MapRecord<String, String, String> record;

    /** The priority level of this processor, used for ordering multiple processors */
    protected int priority;

    private final Class<T> entityClass;
    private final Class<R> repositoryClass;

    /**
     * No-args constructor for AbstractProcessor.
     * Initializes the entity and repository classes using reflection.
     */
    @SuppressWarnings("unchecked")
    protected AbstractProcessor() {
        this.record = null;
        this.entityClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        this.repositoryClass = (Class<R>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1];
    }

    /**
     * Constructor with record parameter.
     * Initializes the processor with a Redis Stream record and sets default priority.
     *
     * @param record the Redis Stream record to process
     */
    @SuppressWarnings("unchecked")
    public AbstractProcessor(MapRecord<String, String, String> record) {
        this.record = record;
        this.priority = Integer.MAX_VALUE;
        this.entityClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        this.repositoryClass = (Class<R>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1];
    }

    /**
     * Constructor with record and priority parameters.
     * Initializes the processor with a Redis Stream record and specified priority.
     *
     * @param record the Redis Stream record to process
     * @param priority the processing priority for this record
     */
    @SuppressWarnings("unchecked")
    public AbstractProcessor(MapRecord<String, String, String> record, int priority) {
        this.record = record;
        this.priority = priority;
        this.entityClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        this.repositoryClass = (Class<R>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1];
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
     * Returns the Redis Stream record being processed.
     *
     * @return the MapRecord instance
     */
    public MapRecord<String, String, String> getRecord() {
        return record;
    }

    /**
     * Returns the processing priority of this processor.
     *
     * @return the priority value
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Sets the processing priority for this processor.
     *
     * @param priority the priority value to set
     */
    public void setPriority(int priority) {
        this.priority = priority;
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
     * Returns the class of the repository being used.
     *
     * @return the repository class
     */
    public Class<R> getRepositoryClass() {
        return repositoryClass;
    }

    /**
     * Returns a list of repositories that can handle the entity type.
     *
     * @return list of repositories for the entity
     */
    public List<Repository<T, ?>> getRepositories() {
        return getRepositoryFinder().findRepositoriesForEntity(getRepositoryClass().getSimpleName());
    }

    /**
     * Processes a Redis Stream record by performing the appropriate CRUD operation
     * on all relevant repositories. Supports both save and delete operations.
     *
     * @param record the Redis Stream record to process
     * @throws ProcessMessageException if an error occurs during processing
     */
    public void process(final MapRecord<String, String, String> record) throws ProcessMessageException {
        List<Repository<T, ?>> repositories = getRepositories();
        AtomicReference<Exception> exception = new AtomicReference<>();
        repositories.forEach(repo -> {
            try {
                String content = record.getValue().get(EVENT_CONTENT_KEY);
                String operation = record.getValue().get(EVENT_OPERATION_KEY);
                logger.debug("Processing message: {}", record.getId());
                logger.debug("Processing EVENT_CONTENT_KEY: {}", content);
                logger.debug("Processing EVENT_OPERATION_KEY: {}", operation);
                if (Misc.Operation.DELETE.getValue().equals(operation)) {
                    logger.trace("Deleting message: {}", record.getId());
                    getRepositoryFinder().executeIdOperation(repo, content, CrudRepository::deleteById);
                    logger.trace("Deleted message: {}", record.getId());
                } else {
                    logger.trace("Saving message: {}", record.getId());
                    getRepositoryFinder().executeOperation(repo, convertToEntity(content), CrudRepository::save);
                    logger.trace("Saved message: {}", record.getId());
                }
                logger.info("Processed message: {}", record.getId());
            } catch (Exception e) {
                logger.error("Error processing message: {}\n{}", record.getId(), e.getMessage());
                exception.set(e);
            }
        });
        if (exception.get() != null) {
            throw new ProcessMessageException(exception.get());
        }
    }

    /**
     * Acknowledges a processed Redis Stream record in all relevant repositories.
     * This marks the message as successfully processed in the Redis Stream.
     *
     * @param record the Redis Stream record to acknowledge
     * @throws AcknowledgeMessageException if an error occurs during acknowledgment
     */
    public void acknowledge(final MapRecord<String, String, String> record) throws AcknowledgeMessageException {
        List<Repository<T, ?>> repositories = getRepositories();
        AtomicReference<Exception> exception = new AtomicReference<>();
        repositories.forEach(repo -> {
            try {
                logger.debug("Acknowledging message: {}", record.getId());
                getRedisTemplate().opsForStream().acknowledge(getConsumerGroup(repositoryClass), record);
                logger.debug("Acknowledged message: {} for group: {}", record.getId(), getConsumerGroup(repositoryClass));
            } catch (Exception e) {
                logger.error("Error acknowledging message: {}\n{}", record.getId(), e.getMessage());
                // will be picked up by the processPendingMessages method
                exception.set(e);
            }
        });
        if (exception.get() != null) {
            throw new AcknowledgeMessageException(exception.get());
        }
    }

}
