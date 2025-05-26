package com.foogaro.kinexis.core.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.foogaro.kinexis.core.service.AnnotationFinder;
import com.foogaro.kinexis.core.service.BeanFinder;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import redis.clients.jedis.Jedis;

import java.time.Duration;

/**
 * Spring configuration class that provides beans for Redis connection, message handling,
 * and utility services. This class configures:
 * <ul>
 *     <li>Redis connection factory and template</li>
 *     <li>Object mapper for JSON serialization</li>
 *     <li>Bean and annotation finders</li>
 *     <li>Stream message listener container</li>
 *     <li>Redis key expiration events</li>
 *     <li>Redis message listener container</li>
 * </ul>
 */
@Configuration
public class KinexisConfiguration {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Value("${spring.data.redis.host}") private String redis_host;
    @Value("${spring.data.redis.port}") private Integer redis_port;

    /**
     * Default constructor for KinexisConfiguration.
     * This constructor is used by Spring to create instances of this configuration class.
     */
    public KinexisConfiguration() {
    }

    /**
     * Creates a Jedis connection factory for Redis if one doesn't already exist.
     * The factory is configured using the host and port from application properties.
     *
     * @return a configured JedisConnectionFactory instance
     */
    @Bean
    @ConditionalOnMissingBean(JedisConnectionFactory.class)
    public JedisConnectionFactory redisConnectionFactory() {
        logger.debug("Creating JedisConnectionFactory");
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(redis_host);
        config.setPort(redis_port);
        logger.debug("Created JedisConnectionFactory");
        return new JedisConnectionFactory(config);
    }

    /**
     * Creates a Redis template for string operations if one doesn't already exist.
     * The template is configured with appropriate serializers for keys and values.
     *
     * @param connectionFactory the Redis connection factory to use
     * @return a configured RedisTemplate instance
     */
    @Bean
    @ConditionalOnMissingBean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory connectionFactory) {
        logger.debug("Creating RedisTemplate");
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new GenericToStringSerializer<>(String.class));
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new StringRedisSerializer());
        logger.debug("Created RedisTemplate: {}", template);
        return template;
    }

    /**
     * Creates an ObjectMapper instance if one doesn't already exist.
     * The mapper is configured to ignore unknown properties during deserialization.
     *
     * @return a configured ObjectMapper instance
     */
    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper;
    }

    /**
     * Creates a BeanFinder instance if one doesn't already exist.
     * The BeanFinder is used to locate Spring beans in the application context.
     *
     * @param listableBeanFactory the Spring bean factory
     * @return a new BeanFinder instance
     */
    @Bean
    @ConditionalOnMissingBean
    public BeanFinder beanFinder(ListableBeanFactory listableBeanFactory) {
        return new BeanFinder(listableBeanFactory);
    }

    /**
     * Creates an AnnotationFinder instance if one doesn't already exist.
     * The AnnotationFinder is used to locate and analyze annotations in the application.
     *
     * @return a new AnnotationFinder instance
     */
    @Bean
    @ConditionalOnMissingBean
    public AnnotationFinder annotationFinder() {
        return new AnnotationFinder();
    }

    /**
     * Creates a StreamMessageListenerContainer for handling Redis Stream messages.
     * The container is configured with a poll timeout and batch size for message processing.
     *
     * @param connectionFactory the Redis connection factory to use
     * @return a configured StreamMessageListenerContainer instance
     */
    @Bean
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer(
            RedisConnectionFactory connectionFactory) {
        logger.debug("Creating StreamMessageListenerContainer");
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                        .builder()
                        .pollTimeout(Duration.ofMillis(1000))
                        .batchSize(100)
                        .build();
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer = StreamMessageListenerContainer.create(connectionFactory, options);
        logger.debug("Created StreamMessageListenerContainer: {}", streamMessageListenerContainer);
        return streamMessageListenerContainer;
    }

    /**
     * Configures Redis key expiration events.
     * This bean enables the 'Ex' notification type for key expiration events.
     *
     * @param jedis the Jedis client instance
     * @return an anonymous object that configures Redis on initialization
     */
    @Bean
    public Object configureRedisKeyExpirationEvents(Jedis jedis) {
        return new Object() {
            @PostConstruct
            public void init() {
                String result = jedis.configSet("notify-keyspace-events", "Ex");
                logger.debug("Redis key expiration events configuration result: " + result);
            }
        };
    }

    /**
     * Creates a RedisMessageListenerContainer for handling Redis pub/sub messages.
     * The container is configured with the provided connection factory.
     *
     * @param redisConnectionFactory the Redis connection factory to use
     * @return a configured RedisMessageListenerContainer instance
     */
    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(
            JedisConnectionFactory redisConnectionFactory) {
        logger.debug("Creating RedisMessageListenerContainer");
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(redisConnectionFactory);
        logger.debug("Created RedisMessageListenerContainer");
        return container;
    }

}
