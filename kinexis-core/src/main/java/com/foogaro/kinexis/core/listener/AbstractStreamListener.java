package com.foogaro.kinexis.core.listener;

import com.foogaro.kinexis.core.orchestrator.ProcessOrchestrator;
import com.foogaro.kinexis.core.processor.Processor;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.*;

import java.lang.reflect.ParameterizedType;
import java.util.Collections;

import static com.foogaro.kinexis.core.Misc.*;

/**
 * Abstract base class for Redis Stream listeners that provides common functionality
 * for handling Redis Stream messages. This class implements the basic infrastructure
 * for listening to Redis Streams, including stream and consumer group creation,
 * message processing, and error handling.
 *
 * @param <T> the type of entity that this listener handles
 * @param <R> the type of repository used for entity operations
 */
public abstract class AbstractStreamListener<T, R> implements StreamListener<T, R> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Class<T> entityClass;
    private final Class<R> repositoryClass;

    /**
     * Gets the process orchestrator that coordinates the message processing flow.
     * This method must be implemented by concrete subclasses.
     *
     * @return the ProcessOrchestrator instance
     */
    public abstract ProcessOrchestrator<T, R> getProcessOrchestrator();

    /**
     * Gets the processor that handles the actual message processing logic.
     * This method must be implemented by concrete subclasses.
     *
     * @return the Processor instance
     */
    public abstract Processor<T, R> getProcessor();

    /**
     * Constructs a new AbstractStreamListener and initializes the entity and repository classes
     * using reflection to determine the generic type parameters.
     *
     * @throws ClassCastException if the generic type parameters cannot be determined
     */
    @SuppressWarnings("unchecked")
    protected AbstractStreamListener() {
        this.entityClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        this.repositoryClass = (Class<R>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1];
    }

    /**
     * Gets the entity class that this listener handles.
     *
     * @return the entity class
     */
    protected Class<T> getEntityClass() {
        return this.entityClass;
    }

    /**
     * Gets the repository class that manages the entity.
     *
     * @return the repository class
     */
    protected Class<R> getRepositoryClass() {
        return repositoryClass;
    }

    /**
     * Initializes the Redis Stream listener after construction.
     * This method:
     * 1. Creates or verifies the existence of the consumer group
     * 2. Creates the stream if it doesn't exist
     * 3. Sets up the message listener
     * 4. Starts the listener container
     */
    @PostConstruct
    private void startListening() {
        logger.info("Starting to listen on stream {} for entity {} managed by repository {}", getStreamKey(getEntityClass()), getEntityClass().getSimpleName(), getRepositoryClass().getSimpleName());

        try {
            getRedisTemplate().opsForStream().createGroup(getStreamKey(getEntityClass()), ReadOffset.from("0"), getConsumerGroup(getRepositoryClass()));
            logger.info("Consumer group {} created for stream {}", getConsumerGroup(getRepositoryClass()), getStreamKey(getEntityClass()));
        } catch (Throwable e) {
            if (!e.getMessage().contains("BUSYGROUP")) {
                if (e.getMessage().contains("NOGROUP")) {
                    getRedisTemplate().opsForStream().add(StreamRecords.newRecord()
                            .in(getStreamKey(getEntityClass()))
                            .ofMap(Collections.singletonMap("init", "true")));

                    getRedisTemplate().opsForStream().createGroup(getStreamKey(getEntityClass()), ReadOffset.lastConsumed(), getConsumerGroup(getRepositoryClass()));
                    logger.info("Stream {} and consumer group {} created", getStreamKey(getEntityClass()), getConsumerGroup(getRepositoryClass()));
                } else {
                    throw e;
                }
            }
        }
        getStreamMessageListenerContainer().receive(
                Consumer.from(getConsumerGroup(getRepositoryClass()), getConsumerName(getEntityClass(), getRepositoryClass())),
                StreamOffset.create(getStreamKey(getEntityClass()), ReadOffset.lastConsumed()),
                this::onMessage
        );

        getStreamMessageListenerContainer().start();
        logger.info("Listener started for stream {} for entity {} managed by repository {}", getStreamKey(getEntityClass()), getEntityClass().getSimpleName(), getRepositoryClass().getSimpleName());
    }

    /**
     * Handles incoming Redis Stream messages by delegating to the process orchestrator.
     * This method processes the message and handles any exceptions that occur during processing.
     *
     * @param record the Redis Stream record to process
     */
    @Override
    public void onMessage(MapRecord<String, String, String> record) {
        try {
            getProcessOrchestrator().orchestrate(record, getProcessor());
        } catch (Exception e) {
            logger.error("Error processing record: {}", record.getId(), e);
        }
    }

}
