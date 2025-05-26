package com.foogaro.kinexis.core.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foogaro.kinexis.core.orchestrator.ProcessOrchestrator;
import com.foogaro.kinexis.core.processor.Processor;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

/**
 * Interface for listening to Redis Stream messages and processing them.
 * This interface defines the contract for components that need to handle
 * incoming messages from Redis Streams, providing access to necessary
 * dependencies for message processing and orchestration.
 *
 * @param <T> the type of entity that this listener handles
 * @param <R> the type of repository used for entity operations
 */
public interface StreamListener<T, R> {

    /**
     * Handles an incoming message from a Redis Stream.
     * This method is called when a new message arrives in the stream
     * and should implement the logic for processing the message.
     *
     * @param message the Redis Stream message to process
     */
    void onMessage(MapRecord<String, String, String> message);

    /**
     * Gets the Redis template used for Redis operations.
     *
     * @return the RedisTemplate instance
     */
    RedisTemplate<String, String> getRedisTemplate();

    /**
     * Gets the stream message listener container that manages
     * the Redis Stream subscription and message delivery.
     *
     * @return the StreamMessageListenerContainer instance
     */
    StreamMessageListenerContainer<String, MapRecord<String, String, String>> getStreamMessageListenerContainer();

    /**
     * Gets the ObjectMapper used for JSON serialization/deserialization.
     *
     * @return the ObjectMapper instance
     */
    ObjectMapper getObjectMapper();

    /**
     * Gets the process orchestrator that coordinates the message processing flow.
     *
     * @return the ProcessOrchestrator instance
     */
    ProcessOrchestrator<T, R> getProcessOrchestrator();

    /**
     * Gets the processor that handles the actual message processing logic.
     *
     * @return the Processor instance
     */
    Processor<T, R> getProcessor();

}
