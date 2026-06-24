package com.foogaro.kinexis.demo.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foogaro.kinexis.core.Misc;
import com.foogaro.kinexis.core.config.KinexisProperties;
import com.foogaro.kinexis.core.exception.ProcessMessageException;
import com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler;
import com.foogaro.kinexis.core.model.KinexisEvent;
import com.foogaro.kinexis.core.processor.AbstractProcessor;
import com.foogaro.kinexis.core.store.CacheStore;
import com.foogaro.kinexis.core.store.EntityStore;
import com.foogaro.kinexis.core.store.EntityStoreRegistry;
import com.foogaro.kinexis.demo.mongodb.entity.Employer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MongodbDemoPendingPropertiesIntegrationTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private static LettuceConnectionFactory connectionFactory;

    private RedisTemplate<String, String> redisTemplate;
    private ObjectMapper objectMapper;
    private InMemoryEmployerStore store;
    private TestProcessor processor;
    private KinexisProperties properties;

    @BeforeEach
    void setUp() throws Exception {
        if (connectionFactory == null) {
            RedisStandaloneConfiguration redisConfiguration = new RedisStandaloneConfiguration(
                    REDIS.getHost(),
                    REDIS.getMappedPort(6379));
            connectionFactory = new LettuceConnectionFactory(redisConfiguration);
            connectionFactory.afterPropertiesSet();
        }

        redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(connectionFactory);
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new StringRedisSerializer());
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashValueSerializer(new StringRedisSerializer());
        redisTemplate.afterPropertiesSet();
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();

        objectMapper = new ObjectMapper();
        store = new InMemoryEmployerStore();
        processor = new TestProcessor();
        inject(processor, "redisTemplate", redisTemplate);
        inject(processor, "objectMapper", objectMapper);
        inject(processor, "entityStoreRegistry", new SingleStoreRegistry(store));
        properties = demoApplicationProperties();
    }

    @AfterAll
    static void closeRedisConnection() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @Test
    void applicationPropertiesBindToPendingConfiguration() throws Exception {
        Properties rawProperties = PropertiesLoaderUtils.loadProperties(new ClassPathResource("application.properties"));

        assertFalse(rawProperties.stringPropertyNames().stream()
                .anyMatch(name -> name.startsWith("kinexis.stream.listener.pel.")));
        assertEquals("3", rawProperties.getProperty("kinexis.stream.listener.pending.max-attempts"));
        assertEquals("120000", rawProperties.getProperty("kinexis.stream.listener.pending.max-retention"));
        assertEquals("50", rawProperties.getProperty("kinexis.stream.listener.pending.batch-size"));
        assertEquals("60000", rawProperties.getProperty("kinexis.stream.listener.pending.fixed-delay"));
        assertEquals(3, properties.getStream().getListener().getPending().getMaxAttempts());
        assertEquals(120000, properties.getStream().getListener().getPending().getMaxRetention());
        assertEquals(50, properties.getStream().getListener().getPending().getBatchSize());
        assertEquals(60000, properties.getStream().getListener().getPending().getFixedDelay());
    }

    @Test
    void pendingScheduleUsesPendingFixedDelayProperty() throws Exception {
        Method method = AbstractPendingMessageHandler.class.getMethod("processPendingMessages");
        Scheduled scheduled = method.getAnnotation(Scheduled.class);

        assertNotNull(scheduled);
        assertEquals("${kinexis.stream.listener.pending.fixed-delay:300000}", scheduled.fixedDelayString());
    }

    @Test
    void pendingHandlerUsesConfiguredBatchSize() throws Exception {
        enqueuePendingEmployers(51);
        TestPendingMessageHandler handler = pendingHandler();

        handler.processPendingMessages();

        assertEquals(50, store.size());
        assertEquals(1, pendingCount());
        assertEquals(50, handler.batchSize());
    }

    @Test
    void pendingHandlerUsesConfiguredRetryAndDlqSettings() throws Exception {
        store.failSaves = true;
        List<MapRecord<String, Object, Object>> records = enqueuePendingEmployers(1);
        String messageId = records.getFirst().getId().getValue();
        TestPendingMessageHandler handler = pendingHandler();

        handler.processPendingMessages();
        Long ttl = redisTemplate.getExpire(Misc.getStreamKey(Employer.class) + Misc.KEY_SEPARATOR + messageId, TimeUnit.MILLISECONDS);
        assertNotNull(ttl);
        assertTrue(ttl > 0);
        assertTrue(ttl <= 120000);
        assertEquals(1, pendingCount());
        assertTrue(dlqRecords().isEmpty());

        handler.processPendingMessages();
        assertEquals(1, pendingCount());
        assertTrue(dlqRecords().isEmpty());

        handler.processPendingMessages();

        assertEquals(0, pendingCount());
        List<MapRecord<String, Object, Object>> dlqRecords = dlqRecords();
        assertEquals(1, dlqRecords.size());
        Map<Object, Object> dlq = dlqRecords.getFirst().getValue();
        assertEquals("Too many attempts", dlq.get(AbstractPendingMessageHandler.DLQ_REASON_KEY));
        assertEquals("3", dlq.get(AbstractPendingMessageHandler.DLQ_ATTEMPTS_KEY));
        assertEquals("mongodb", dlq.get(AbstractPendingMessageHandler.DLQ_FAILED_STORE_KEY));
        assertEquals(ProcessMessageException.class.getName(), dlq.get(AbstractPendingMessageHandler.DLQ_EXCEPTION_CLASS_KEY));
        assertNotNull(dlq.get(AbstractPendingMessageHandler.DLQ_FAILURE_TIMESTAMP_KEY));
        assertEquals(3, handler.maxAttempts());
        assertEquals(Duration.ofMinutes(2).toMillis(), handler.maxRetention());
    }

    private TestPendingMessageHandler pendingHandler() throws Exception {
        TestPendingMessageHandler handler = new TestPendingMessageHandler(processor);
        inject(handler, "redisTemplate", redisTemplate);
        handler.setKinexisProperties(properties);
        return handler;
    }

    private List<MapRecord<String, Object, Object>> enqueuePendingEmployers(int count) throws Exception {
        String streamKey = Misc.getStreamKey(Employer.class);
        for (int i = 1; i <= count; i++) {
            Employer employer = new Employer((long) i, "Employer " + i, "Address " + i,
                    "employer-" + i + "@kinexis.local", "+39000" + i);
            RecordId recordId = redisTemplate.opsForStream().add(StreamRecords.newRecord()
                    .withId(RecordId.autoGenerate())
                    .ofMap(KinexisEvent.save(Employer.class, objectMapper.writeValueAsString(employer)).toRecordMap())
                    .withStreamKey(streamKey));
            assertNotNull(recordId);
        }

        redisTemplate.opsForStream().createGroup(streamKey, ReadOffset.from("0"), Misc.getConsumerGroup(Employer.class));
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                Consumer.from(Misc.getConsumerGroup(Employer.class), Misc.getConsumerName(Employer.class)),
                StreamReadOptions.empty().count(count),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()));
        assertNotNull(records);
        assertEquals(count, records.size());
        assertEquals(count, pendingCount());
        return records;
    }

    private long pendingCount() {
        PendingMessagesSummary pending = redisTemplate.opsForStream()
                .pending(Misc.getStreamKey(Employer.class), Misc.getConsumerGroup(Employer.class));
        return pending == null ? 0 : pending.getTotalPendingMessages();
    }

    private List<MapRecord<String, Object, Object>> dlqRecords() {
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream()
                .range(Misc.getDLQStreamKey(Employer.class), Range.unbounded());
        return records == null ? List.of() : records;
    }

    private KinexisProperties demoApplicationProperties() throws Exception {
        Properties rawProperties = PropertiesLoaderUtils.loadProperties(new ClassPathResource("application.properties"));
        Map<String, Object> values = new HashMap<>();
        rawProperties.forEach((key, value) -> values.put(String.valueOf(key), value));
        return new Binder(new MapConfigurationPropertySource(values))
                .bind("kinexis", Bindable.of(KinexisProperties.class))
                .orElseThrow(() -> new IllegalStateException("Could not bind demo Kinexis properties"));
    }

    private static void inject(Object target, String fieldName, Object value) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Field findField(Class<?> type, String fieldName) throws NoSuchFieldException {
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    private static final class TestProcessor extends AbstractProcessor<Employer> {
    }

    private static final class TestPendingMessageHandler extends AbstractPendingMessageHandler<Employer> {

        private final TestProcessor processor;

        private TestPendingMessageHandler(TestProcessor processor) {
            this.processor = processor;
        }

        @Override
        public TestProcessor getProcessor() {
            return processor;
        }

        private int maxAttempts() {
            return MAX_ATTEMPTS;
        }

        private long maxRetention() {
            return MAX_RETENTION;
        }

        private int batchSize() {
            return BATCH_SIZE;
        }
    }

    private static final class SingleStoreRegistry implements EntityStoreRegistry {

        private final InMemoryEmployerStore store;

        private SingleStoreRegistry(InMemoryEmployerStore store) {
            this.store = store;
        }

        @Override
        public <T> Optional<CacheStore<T>> findCacheStore(Class<T> entityType) {
            return Optional.empty();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<EntityStore<T>> findPrimaryStore(Class<T> entityType) {
            return Employer.class.equals(entityType) ? Optional.of((EntityStore<T>) store) : Optional.empty();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> List<EntityStore<T>> findTargetStores(Class<T> entityType) {
            return Employer.class.equals(entityType) ? List.of((EntityStore<T>) store) : List.of();
        }

        @Override
        public <T> List<EntityStore<T>> findTargetStores(Class<T> entityType, Collection<String> targets) {
            return findTargetStores(entityType);
        }
    }

    private static final class InMemoryEmployerStore implements EntityStore<Employer> {

        private final Map<Object, Employer> employers = new ConcurrentHashMap<>();
        private boolean failSaves;

        @Override
        public String name() {
            return "mongodb";
        }

        @Override
        public Class<Employer> entityType() {
            return Employer.class;
        }

        @Override
        public Set<String> targets() {
            return Set.of("mongodb", "primary");
        }

        @Override
        public Optional<Employer> findById(Object id) {
            return Optional.ofNullable(employers.get(id));
        }

        @Override
        public Employer save(Employer entity) {
            if (failSaves) {
                throw new IllegalStateException("MongoDB demo store failure");
            }
            employers.put(entity.getId(), entity);
            return entity;
        }

        @Override
        public void deleteById(Object id) {
            employers.remove(id);
        }

        private int size() {
            return employers.size();
        }
    }
}
