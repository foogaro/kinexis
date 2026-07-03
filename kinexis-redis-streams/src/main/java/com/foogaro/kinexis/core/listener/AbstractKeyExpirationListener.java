package com.foogaro.kinexis.core.listener;

import com.foogaro.kinexis.core.service.KinexisService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.util.Optional;

/**
 * Abstract base class for Redis key expiration listeners.
 * This class provides the core functionality to handle expired keys in Redis and reload the corresponding entities.
 * It listens for key expiration events and processes them based on a configurable key prefix.
 *
 * @param <T> the type of entity that this listener handles
 */
public abstract class AbstractKeyExpirationListener<T> implements MessageListener {

    /**
     * No-args constructor for AbstractKeyExpirationListener.
     */
    protected AbstractKeyExpirationListener() {}

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private RedisMessageListenerContainer redisMessageListenerContainer;

    /**
     * Returns the service instance responsible for handling the entity type T.
     * This service is used to reload entities when their corresponding Redis keys expire.
     *
     * @return the KinexisService instance for the entity type T
     */
    protected abstract KinexisService<T> getService();

    /**
     * Returns the key prefix used to identify relevant Redis keys for this listener.
     * Only keys starting with this prefix will be processed when they expire.
     *
     * @return the string prefix used to identify relevant Redis keys
     */
    protected abstract String getKeyPrefix();

    @PostConstruct
    private void init() {
        redisMessageListenerContainer.addMessageListener(this,
                new PatternTopic("__keyevent@*__:expired"));
        logger.info("{} initialized and listening for expired keys.", getClass().getSimpleName());
    }

    /**
     * Handles Redis key expiration events.
     * When a key expires, this method checks if the key matches the configured prefix.
     * If it matches, the corresponding entity is reloaded using the service.
     * If it doesn't match, the event is ignored.
     *
     * @param message the Redis message containing the expired key information
     * @param pattern the pattern that matched this message
     */
    public void onMessage(Message message, byte[] pattern) {
        String key = new String(message.getBody());
        logger.debug("Received message on pattern: {}", new String(pattern));
        logger.debug("Received message {} on channel: {}", key, new String(message.getChannel()));

        if (key.startsWith(getKeyPrefix())) {
            logger.debug("Processing expired key: {}", key);
            String id = key.substring(getKeyPrefix().length());
            Optional<T> reloadedEntity = getService().findById(id);
            logger.debug("Expired entity({}) reloaded: {}", key, reloadedEntity);
        } else {
            logger.debug("Ignoring expired key (prefix {} do not match with the key): {}", getKeyPrefix(), key);
        }
    }

}
