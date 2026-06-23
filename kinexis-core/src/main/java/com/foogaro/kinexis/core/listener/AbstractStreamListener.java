package com.foogaro.kinexis.core.listener;

import com.foogaro.kinexis.core.processor.Processor;
import com.foogaro.kinexis.core.stream.KinexisStreamLifecycle;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;

import java.lang.reflect.ParameterizedType;

import static com.foogaro.kinexis.core.Misc.*;

/**
 * Abstract base class for Redis Stream listeners that provides common functionality
 * for handling Redis Stream messages. This class implements the basic infrastructure
 * for listening to Redis Streams, including stream and consumer group creation,
 * message processing, and error handling.
 *
 * @param <T> the type of entity that this listener handles
 */
public abstract class AbstractStreamListener<T>
        implements org.springframework.data.redis.stream.StreamListener<String, MapRecord<String, String, String>> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Class<T> entityClass;

    @Autowired
    private KinexisStreamLifecycle streamLifecycle;

    /**
     * Gets the processor that handles the actual message processing logic.
     * This method must be implemented by concrete subclasses.
     *
     * @return the Processor instance
     */
    public abstract Processor<T> getProcessor();

    /**
     * Constructs a new AbstractStreamListener and initializes the entity class
     * using reflection to determine the generic type parameters.
     *
     * @throws ClassCastException if the generic type parameters cannot be determined
     */
    @SuppressWarnings("unchecked")
    protected AbstractStreamListener() {
        this.entityClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
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
     * Initializes the Redis Stream listener after construction.
     * This method:
     * 1. Creates or verifies the existence of the consumer group
     * 2. Creates the stream if it doesn't exist
     * 3. Sets up the message listener
     * 4. Starts the listener container
     */
    @PostConstruct
    private void startListening() {
        logger.info("Starting to listen on stream {} for entity {}", getStreamKey(getEntityClass()), getEntityClass().getSimpleName());

        streamLifecycle.receive(
                getStreamKey(getEntityClass()),
                getConsumerGroup(getEntityClass()),
                getConsumerName(getEntityClass()),
                this::onMessage);
        logger.info("Listener started for stream {} for entity {}", getStreamKey(getEntityClass()), getEntityClass().getSimpleName());
    }

    /**
     * Handles incoming Redis Stream messages by processing and acknowledging them.
     *
     * @param record the Redis Stream record to process
     */
    @Override
    public void onMessage(MapRecord<String, String, String> record) {
        try {
            Processor<T> processor = getProcessor();
            processor.process(record);
            processor.acknowledge(record);
        } catch (Exception e) {
            logger.error("Error processing record: {}", record.getId(), e);
        }
    }

}
