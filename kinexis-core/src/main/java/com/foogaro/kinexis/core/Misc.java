package com.foogaro.kinexis.core;

import com.redis.om.spring.annotations.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisHash;

import java.lang.annotation.Annotation;

/**
 * Utility class providing common functionality for Redis operations and message handling.
 * This class contains methods for generating Redis keys, stream names, consumer groups,
 * and handling message operations. It also provides constants for message keys and
 * separators used throughout the application.
 */
public class Misc {

    private static final Logger logger = LoggerFactory.getLogger(Misc.class);

    /** Key for accessing the content of a message in a MapRecord */
    public final static String EVENT_CONTENT_KEY = "content";
    /** Key for accessing the operation type of a message in a MapRecord */
    public final static String EVENT_OPERATION_KEY = "operation";

    /** Separator used in Redis keys */
    public final static String KEY_SEPARATOR = ":";
    /** Separator used in Redis values */
    public final static String VALUE_SEPARATOR = "_";

    /** Prefix for Redis Stream keys */
    private final static String STREAM_KEY_PREFIX = "wb:stream:entity:";
    /** Suffix for Dead Letter Queue (DLQ) Stream keys */
    private final static String STREAM_KEY_DLQ_SUFFIX = ":dlq";

    /** Suffix for consumer group names */
    public final static String CONSUMER_GROUP_SUFFIX = "_group";
    /** Suffix for consumer names */
    public final static String CONSUMER_SUFFIX = "_consumer";

    /**
     * Default constructor for Misc.
     * This constructor is used to create instances of this utility class.
     */
    public Misc() {
    }

    /**
     * Gets the entity key prefix for a given entity class.
     * The prefix is determined by checking for {@link RedisHash} or {@link Document} annotations.
     * If neither annotation is present, the class name in lowercase is used.
     *
     * @param entityClass the entity class to get the key prefix for
     * @return the key prefix for the entity
     */
    public static String getEntityKeyPrefix(final Class<?> entityClass) {
        try {
            if (entityClass.isAnnotationPresent(RedisHash.class)) {
                Annotation annotation = entityClass.getAnnotation(RedisHash.class);
                return (String) RedisHash.class.getMethod("value").invoke(annotation);
            }
            
            if (entityClass.isAnnotationPresent(Document.class)) {
                Annotation annotation = entityClass.getAnnotation(Document.class);
                return (String) Document.class.getMethod("value").invoke(annotation);
            }
        } catch (NoSuchMethodException | IllegalAccessException | java.lang.reflect.InvocationTargetException e) {
            logger.debug("Annotation class not found or error accessing annotation value: {}", e.getMessage());
        }

        return entityClass.getSimpleName().toLowerCase();
    }

    /**
     * Generates a Redis Stream key for an entity class.
     *
     * @param entityClass the entity class to generate the stream key for
     * @return the stream key for the entity
     */
    public static String getStreamKey(final Class<?> entityClass) {
        return STREAM_KEY_PREFIX + entityClass.getSimpleName().toLowerCase();
    }

    /**
     * Generates a Dead Letter Queue (DLQ) Stream key for an entity class.
     *
     * @param entityClass the entity class to generate the DLQ stream key for
     * @return the DLQ stream key for the entity
     */
    public static String getDLQStreamKey(final Class<?> entityClass) {
        return STREAM_KEY_PREFIX + entityClass.getSimpleName().toLowerCase() + STREAM_KEY_DLQ_SUFFIX;
    }

    /**
     * Generates a consumer group name for a repository class.
     *
     * @param repositoryClass the repository class to generate the consumer group name for
     * @return the consumer group name
     */
    public static String getConsumerGroup(final Class<?> repositoryClass) {
        return repositoryClass.getSimpleName().toLowerCase() + CONSUMER_GROUP_SUFFIX;
    }

    /**
     * Generates a consumer name combining entity and repository class names.
     *
     * @param entityClass the entity class
     * @param repositoryClass the repository class
     * @return the consumer name
     */
    public static String getConsumerName(final Class<?> entityClass, final Class<?> repositoryClass) {
        return entityClass.getSimpleName().toLowerCase() + VALUE_SEPARATOR + repositoryClass.getSimpleName().toLowerCase() + CONSUMER_SUFFIX;
    }

    /**
     * Logs the contents of a Redis Stream message for debugging purposes.
     * Logs the stream ID, message ID, content, and operation type.
     *
     * @param message the MapRecord message to dump
     */
    public static void dumpMessage(final MapRecord<String, String, String> message) {
        try {
            logger.debug("Stream ID: {}", message.getStream());
            logger.debug("Message ID: {}", message.getId());
            logger.debug("Message.Content: {}", message.getValue().get(EVENT_CONTENT_KEY));
            logger.debug("Message.Operation: {}", message.getValue().get(EVENT_OPERATION_KEY));
        } catch (Exception e) {
            logger.error("Error dumping message: {}", message, e);
        }
    }

    /**
     * Enum representing the possible operations that can be performed on entities.
     * These operations are used in Redis Stream messages to indicate the type of action.
     */
    public enum Operation {
        /** Create operation */
        CREATE("CREATE"),
        /** Read operation */
        READ("READ"),
        /** Update operation */
        UPDATE("UPDATE"),
        /** Delete operation */
        DELETE("DELETE");

        private final String value;

        /**
         * Creates a new Operation enum value.
         *
         * @param value the string representation of the operation
         */
        Operation(String value) {
            this.value = value;
        }

        /**
         * Gets the string value of the operation.
         *
         * @return the string value
         */
        public String getValue() {
            return value;
        }

        /**
         * Converts a string to an Operation enum value.
         *
         * @param text the string to convert
         * @return the corresponding Operation enum value
         * @throws IllegalArgumentException if no matching operation is found
         */
        public static Operation fromString(final String text) {
            for (Operation operation : Operation.values()) {
                if (operation.value.equalsIgnoreCase(text)) {
                    return operation;
                }
            }
            throw new IllegalArgumentException("No constant with text " + text + " found");
        }

        @Override
        public String toString() {
            return this.value;
        }
    }
}
