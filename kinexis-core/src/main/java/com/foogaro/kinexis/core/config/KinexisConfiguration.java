package com.foogaro.kinexis.core.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.foogaro.kinexis.core.service.AnnotationFinder;
import com.foogaro.kinexis.core.service.BeanFinder;
import com.foogaro.kinexis.core.service.KinexisDlqService;
import com.foogaro.kinexis.core.service.KinexisDiagnosticsService;
import com.foogaro.kinexis.core.service.KinexisEntityRegistry;
import com.foogaro.kinexis.core.service.KinexisService;
import com.foogaro.kinexis.core.service.KinexisStoreValidator;
import com.foogaro.kinexis.core.processor.Processor;
import com.foogaro.kinexis.core.store.BeanFinderEntityStoreRegistry;
import com.foogaro.kinexis.core.store.DefaultEntityStoreRegistry;
import com.foogaro.kinexis.core.store.EmptyEntityStoreRegistry;
import com.foogaro.kinexis.core.store.EntityStore;
import com.foogaro.kinexis.core.store.EntityStoreRegistry;
import com.foogaro.kinexis.core.stream.EventPublisher;
import com.foogaro.kinexis.core.stream.KinexisStreamLifecycle;
import com.foogaro.kinexis.core.stream.RedisStreamEventPublisher;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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
@EnableConfigurationProperties(KinexisProperties.class)
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

    @Bean
    public ClientResources clientResources() {
        return DefaultClientResources.builder()
                .ioThreadPoolSize(4)
                .computationThreadPoolSize(4)
                .build();
    }

    @Bean
    public ClientOptions clientOptions() {
        return ClientOptions.builder()
                .socketOptions(SocketOptions.builder()
                        .keepAlive(true)
                        .tcpNoDelay(true)
                        .build())
                .autoReconnect(true)
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.ACCEPT_COMMANDS)
                .build();
    }

    @Bean
    @Primary
    public LettuceConnectionFactory lettuceConnectionFactory(ClientResources clientResources, ClientOptions clientOptions) {
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                .clientResources(clientResources)
                .clientOptions(clientOptions)
                .commandTimeout(Duration.ofSeconds(5))
                .shutdownTimeout(Duration.ZERO)
                .build();

        RedisStandaloneConfiguration serverConfig = new RedisStandaloneConfiguration(redis_host, redis_port);

        return new LettuceConnectionFactory(serverConfig, clientConfig);
    }

    /**
     * Creates a Redis template for string operations if one doesn't already exist.
     * The template is configured with appropriate serializers for keys and values.
     *
     * @param connectionFactory the Redis connection factory to use
     * @return a configured RedisTemplate instance
     */
    @Bean
    public RedisTemplate<String, String> redisTemplate(LettuceConnectionFactory connectionFactory) {
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

    @Bean
    public StringRedisTemplate stringRedisTemplate(LettuceConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
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

    @Bean
    @ConditionalOnMissingBean
    @SuppressWarnings("deprecation")
    public EntityStoreRegistry entityStoreRegistry(BeanFinder beanFinder, ObjectProvider<EntityStore<?>> entityStores,
                                                   RedisTemplate<String, String> redisTemplate,
                                                   KinexisProperties properties) {
        EntityStoreRegistry fallbackRegistry = properties.getStores().getRepositoryDiscovery().isEnabled()
                ? new BeanFinderEntityStoreRegistry(beanFinder, redisTemplate)
                : new EmptyEntityStoreRegistry();
        return new DefaultEntityStoreRegistry(entityStores.orderedStream().toList(), fallbackRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public EventPublisher eventPublisher(RedisTemplate<String, String> redisTemplate) {
        return new RedisStreamEventPublisher(redisTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public KinexisDlqService kinexisDlqService(RedisTemplate<String, String> redisTemplate) {
        return new KinexisDlqService(redisTemplate);
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "kinexisStoreExecutor")
    public ExecutorService kinexisStoreExecutor(KinexisProperties properties) {
        int parallelism = Math.max(1, properties.getProcessing().getMaxParallelStores());
        AtomicInteger threadCount = new AtomicInteger();
        return Executors.newFixedThreadPool(parallelism, task -> {
            Thread thread = new Thread(task, "kinexis-store-" + threadCount.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    @Bean
    @ConditionalOnMissingBean
    public KinexisDiagnosticsService kinexisDiagnosticsService(ObjectProvider<EntityStore<?>> entityStores,
                                                               ObjectProvider<Processor<?>> processors,
                                                               ObjectProvider<KinexisService<?>> services,
                                                               ObjectProvider<KinexisEntityRegistry> entityRegistries,
                                                               EntityStoreRegistry entityStoreRegistry,
                                                               AnnotationFinder annotationFinder) {
        return new KinexisDiagnosticsService(
                entityStores.orderedStream().toList(),
                processors.orderedStream().toList(),
                services.orderedStream().toList(),
                entityRegistries.orderedStream().toList(),
                entityStoreRegistry,
                annotationFinder);
    }

    @Bean
    @ConditionalOnMissingBean
    public KinexisStoreValidator kinexisStoreValidator(KinexisDiagnosticsService diagnosticsService,
                                                       KinexisProperties properties) {
        return new KinexisStoreValidator(diagnosticsService, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public KinexisStreamLifecycle kinexisStreamLifecycle(
            RedisTemplate<String, String> redisTemplate,
            StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer) {
        return new KinexisStreamLifecycle(redisTemplate, streamMessageListenerContainer);
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
            LettuceConnectionFactory connectionFactory, KinexisProperties properties) {
        logger.debug("Creating StreamMessageListenerContainer");
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                        .builder()
                        .pollTimeout(properties.getStream().getPollTimeout())
                        .batchSize(properties.getStream().getBatchSize())
                        .build();
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer = StreamMessageListenerContainer.create(connectionFactory, options);
        logger.debug("Created StreamMessageListenerContainer: {}", streamMessageListenerContainer);
        return streamMessageListenerContainer;
    }

    /**
     * Configures Redis key expiration events.
     * This bean enables the 'Ex' notification type for key expiration events.
     *
     * @return an anonymous object that configures Redis on initialization
     */
    @Bean
    public Object configureRedisKeyExpirationEvents(RedisConnectionFactory redisConnectionFactory) {
        return new Object() {
            @PostConstruct
            public void init() {
                try (RedisConnection connection = redisConnectionFactory.getConnection()) {
                    connection.serverCommands().setConfig("notify-keyspace-events", "Ex");
                    logger.debug("Redis key expiration events configured");
                }
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
    public RedisMessageListenerContainer redisMessageListenerContainer(LettuceConnectionFactory redisConnectionFactory) {
        logger.debug("Creating RedisMessageListenerContainer");
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(redisConnectionFactory);
        logger.debug("Created RedisMessageListenerContainer");
        return container;
    }

}
