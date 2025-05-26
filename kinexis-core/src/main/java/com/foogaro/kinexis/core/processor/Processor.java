package com.foogaro.kinexis.core.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.foogaro.kinexis.core.exception.AcknowledgeMessageException;
import com.foogaro.kinexis.core.exception.ProcessMessageException;
import com.foogaro.kinexis.core.service.BeanFinder;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.repository.Repository;

import java.util.List;

/**
 * Interface for processing Redis Stream messages.
 * Defines the contract for classes that handle message processing and acknowledgment
 * in a Redis Stream-based system. Implementations should provide functionality for
 * CRUD operations on entities and message acknowledgment.
 *
 * @param <T> the type of entity that this processor handles
 * @param <R> the type of repository used for entity operations
 */
public interface Processor<T, R> {

    /**
     * Returns the bean finder used to locate repositories.
     *
     * @return the BeanFinder instance
     */
    BeanFinder getRepositoryFinder();

    /**
     * Returns the Redis template used for Redis operations.
     *
     * @return the RedisTemplate instance
     */
    RedisTemplate<String, String> getRedisTemplate();

    /**
     * Returns the ObjectMapper used for JSON serialization/deserialization.
     *
     * @return the ObjectMapper instance
     */
    ObjectMapper getObjectMapper();

    /**
     * Converts a JSON string to an entity object.
     *
     * @param content the JSON string to convert
     * @return the converted entity object
     * @throws JsonProcessingException if the JSON string cannot be processed
     */
    T convertToEntity(String content) throws JsonProcessingException;

    /**
     * Returns the Redis Stream record being processed.
     *
     * @return the MapRecord instance
     */
    MapRecord<String, String, String> getRecord();

    /**
     * Returns the processing priority of this processor.
     *
     * @return the priority value
     */
    int getPriority();

    /**
     * Sets the processing priority for this processor.
     *
     * @param priority the priority value to set
     */
    void setPriority(int priority);

    /**
     * Returns the class of the entity being processed.
     *
     * @return the entity class
     */
    Class<T> getEntityClass();

    /**
     * Returns the class of the repository being used.
     *
     * @return the repository class
     */
    Class<R> getRepositoryClass();

    /**
     * Returns a list of repositories that can handle the entity type.
     *
     * @return list of repositories for the entity
     */
    List<Repository<T, ?>> getRepositories();

    /**
     * Processes a Redis Stream record by performing the appropriate CRUD operation
     * on all relevant repositories. Supports both save and delete operations.
     *
     * @param record the Redis Stream record to process
     * @throws ProcessMessageException if an error occurs during processing
     */
    void process(final MapRecord<String, String, String> record) throws ProcessMessageException;

    /**
     * Acknowledges a processed Redis Stream record in all relevant repositories.
     * This marks the message as successfully processed in the Redis Stream.
     *
     * @param record the Redis Stream record to acknowledge
     * @throws AcknowledgeMessageException if an error occurs during acknowledgment
     */
    void acknowledge(final MapRecord<String, String, String> record) throws AcknowledgeMessageException;

}
