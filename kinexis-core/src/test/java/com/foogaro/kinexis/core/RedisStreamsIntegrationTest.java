package com.foogaro.kinexis.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foogaro.kinexis.core.annotation.CachingPatterns;
import com.foogaro.kinexis.core.config.KinexisProperties;
import com.foogaro.kinexis.core.exception.AcknowledgeMessageException;
import com.foogaro.kinexis.core.exception.ProcessMessageException;
import com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler;
import com.foogaro.kinexis.core.model.CachingPattern;
import com.foogaro.kinexis.core.model.KinexisEvent;
import com.foogaro.kinexis.core.processor.AbstractProcessor;
import com.foogaro.kinexis.core.service.AnnotationFinder;
import com.foogaro.kinexis.core.store.CacheStore;
import com.foogaro.kinexis.core.store.BeanFinderEntityStoreRegistry;
import com.foogaro.kinexis.core.store.CrudRepositoryCacheStore;
import com.foogaro.kinexis.core.store.CrudRepositoryEntityStore;
import com.foogaro.kinexis.core.store.DefaultEntityStoreRegistry;
import com.foogaro.kinexis.core.store.EmptyEntityStoreRegistry;
import com.foogaro.kinexis.core.store.EntityStore;
import com.foogaro.kinexis.core.store.EntityStoreRegistry;
import com.foogaro.kinexis.core.store.RedisOmCacheStore;
import com.foogaro.kinexis.core.service.BeanFinder;
import com.foogaro.kinexis.core.service.KinexisDlqService;
import com.foogaro.kinexis.core.service.KinexisDiagnosticsService;
import com.foogaro.kinexis.core.service.KinexisEntityRegistry;
import com.foogaro.kinexis.core.service.KinexisStoreValidator;
import com.foogaro.kinexis.core.stream.RedisStreamEventPublisher;
import com.foogaro.kinexis.core.service.KinexisService;
import com.foogaro.kinexis.core.stream.EventPublisher;
import com.foogaro.kinexis.core.stream.KinexisStreamLifecycle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Range;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.repository.CrudRepository;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.foogaro.kinexis.core.Misc.EVENT_CONTENT_KEY;
import static com.foogaro.kinexis.core.Misc.EVENT_OPERATION_KEY;
import static com.foogaro.kinexis.core.model.KinexisEvent.EVENT_SCHEMA_VERSION_KEY;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class RedisStreamsIntegrationTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private static LettuceConnectionFactory connectionFactory;

    private RedisTemplate<String, String> redisTemplate;
    private ObjectMapper objectMapper;
    private InMemoryStore backingStore;
    private InMemoryCacheStore cacheStore;
    private TestProcessor processor;

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
        backingStore = new InMemoryStore("backing");
        cacheStore = new InMemoryCacheStore("cache");
        processor = new TestProcessor(redisTemplate, objectMapper);
        inject(processor, "entityStoreRegistry", new TestStoreRegistry(cacheStore, backingStore));
    }

    @AfterAll
    static void closeRedisConnection() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @Test
    void savePublishesVersionedRecordToRedisStream() throws Exception {
        RedisStreamEventPublisher publisher = new RedisStreamEventPublisher(redisTemplate);
        TestEntity entity = new TestEntity(1L, "Ada");

        String recordId = publisher.append(TestEntity.class, KinexisEvent.save(TestEntity.class, objectMapper.writeValueAsString(entity)));

        assertNotNull(recordId);
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream()
                .range(Misc.getStreamKey(TestEntity.class), Range.unbounded());
        assertNotNull(records);
        assertEquals(1, records.size());
        assertEquals(Misc.Operation.SAVE.getValue(), records.getFirst().getValue().get(EVENT_OPERATION_KEY));
        assertEquals(KinexisEvent.CURRENT_SCHEMA_VERSION, records.getFirst().getValue().get(EVENT_SCHEMA_VERSION_KEY));
        assertEquals(TestEntity.class.getName(), records.getFirst().getValue().get(KinexisEvent.EVENT_ENTITY_TYPE_KEY));
    }

    @Test
    void processorSavesEntityAndAcknowledgesAfterSuccess() throws Exception {
        MapRecord<String, String, String> record = enqueueAndRead(KinexisEvent.save(TestEntity.class, objectMapper.writeValueAsString(new TestEntity(2L, "Grace"))));

        processor.process(record);
        processor.acknowledge(record);

        assertEquals(Optional.of(new TestEntity(2L, "Grace")), backingStore.findById(2L));
        assertEquals(0, pendingCount());
    }

    @Test
    void defaultRegistryPrefersExplicitStoreBeans() {
        InMemoryStore explicitBackingStore = new InMemoryStore("explicitBacking");
        InMemoryCacheStore explicitCacheStore = new InMemoryCacheStore("explicitCache");
        EntityStoreRegistry registry = new DefaultEntityStoreRegistry(
                List.of(explicitBackingStore, explicitCacheStore),
                new EmptyEntityStoreRegistry());

        assertSame(explicitCacheStore, registry.findCacheStore(TestEntity.class).orElseThrow());
        assertSame(explicitBackingStore, registry.findPrimaryStore(TestEntity.class).orElseThrow());
        assertEquals(List.of(explicitBackingStore), registry.findTargetStores(TestEntity.class, TestRepository.class));
    }

    @Test
    void repositoryDiscoveryIsDisabledByDefault() {
        KinexisProperties properties = new KinexisProperties();
        StaticListableBeanFactory beanFactory = new StaticListableBeanFactory();
        beanFactory.addBean("testRepository", new InMemoryCrudRepository());
        BeanFinder beanFinder = new BeanFinder(beanFactory);

        EntityStoreRegistry fallbackRegistry = properties.getStores().getRepositoryDiscovery().isEnabled()
                ? new BeanFinderEntityStoreRegistry(beanFinder, redisTemplate)
                : new EmptyEntityStoreRegistry();
        EntityStoreRegistry registry = new DefaultEntityStoreRegistry(List.of(), fallbackRegistry);

        assertFalse(properties.getStores().getRepositoryDiscovery().isEnabled());
        assertTrue(registry.findPrimaryStore(TestEntity.class).isEmpty());
        assertTrue(registry.findCacheStore(TestEntity.class).isEmpty());
        assertTrue(registry.findTargetStores(TestEntity.class).isEmpty());
    }

    @Test
    @SuppressWarnings("deprecation")
    void repositoryDiscoveryCanBeEnabledIntentionally() {
        KinexisProperties properties = new KinexisProperties();
        properties.getStores().getRepositoryDiscovery().setEnabled(true);
        StaticListableBeanFactory beanFactory = new StaticListableBeanFactory();
        InMemoryCrudRepository repository = new InMemoryCrudRepository();
        beanFactory.addBean("testRepository", repository);
        BeanFinder beanFinder = new BeanFinder(beanFactory);

        EntityStoreRegistry fallbackRegistry = properties.getStores().getRepositoryDiscovery().isEnabled()
                ? new BeanFinderEntityStoreRegistry(beanFinder, redisTemplate)
                : new EmptyEntityStoreRegistry();
        EntityStoreRegistry registry = new DefaultEntityStoreRegistry(List.of(), fallbackRegistry);

        List<EntityStore<TestEntity>> stores = registry.findTargetStores(TestEntity.class);
        List<EntityStore<TestEntity>> targetedStores = registry.findTargetStores(TestEntity.class, List.of("CrudRepository"));
        assertEquals(1, stores.size());
        assertEquals("CrudRepository", stores.getFirst().name());
        assertEquals(1, targetedStores.size());
        assertEquals("CrudRepository", targetedStores.getFirst().name());

        stores.getFirst().save(new TestEntity(42L, "Discovered"));

        assertEquals(Optional.of(new TestEntity(42L, "Discovered")), repository.findById(42L));
    }

    @Test
    void crudRepositoryStoreBuildersConfigureNamesAndTargetGroups() {
        BeanFinder beanFinder = new BeanFinder(new StaticListableBeanFactory());
        InMemoryCrudRepository repository = new InMemoryCrudRepository();
        CrudRepositoryEntityStore<TestEntity> entityStore = CrudRepositoryEntityStore
                .builder(TestEntity.class, repository, beanFinder)
                .name("mysqlEmployerStore")
                .targets("mysql", "primary")
                .build();
        CrudRepositoryCacheStore<TestEntity> cacheStore = CrudRepositoryCacheStore
                .builder(TestEntity.class, repository, beanFinder)
                .name("redisEmployerCache")
                .targets("redis", "cache")
                .build();
        RedisOmCacheStore<TestEntity> redisOmStore = RedisOmCacheStore
                .builder(TestEntity.class, repository, beanFinder)
                .name("redisOmEmployerCache")
                .target("redis-om")
                .build();
        EntityStoreRegistry registry = new DefaultEntityStoreRegistry(
                List.of(entityStore, cacheStore, redisOmStore),
                new EmptyEntityStoreRegistry());

        assertEquals("mysqlEmployerStore", entityStore.name());
        assertEquals(Set.of("mysql", "primary"), entityStore.targets());
        assertEquals(Set.of("redis", "cache"), cacheStore.targets());
        assertEquals(Set.of("redis-om"), redisOmStore.targets());
        assertEquals(List.of(entityStore), registry.findTargetStores(TestEntity.class, TestRepository.class, List.of("primary")));
        assertEquals(List.of(entityStore), registry.findTargetStores(TestEntity.class, List.of("primary")));
        assertSame(cacheStore, registry.findCacheStore(TestEntity.class).orElseThrow());
    }

    @Test
    void storeBuilderRejectsBlankNameAndFallsBackToStoreNameForEmptyTargets() {
        BeanFinder beanFinder = new BeanFinder(new StaticListableBeanFactory());
        InMemoryCrudRepository repository = new InMemoryCrudRepository();

        assertThrows(IllegalArgumentException.class, () -> CrudRepositoryEntityStore
                .builder(TestEntity.class, repository, beanFinder)
                .name(" ")
                .build());

        CrudRepositoryEntityStore<TestEntity> entityStore = CrudRepositoryEntityStore
                .builder(TestEntity.class, repository, beanFinder)
                .name("primaryStore")
                .targets("", " ")
                .build();

        assertEquals(Set.of("primaryStore"), entityStore.targets());
    }

    @Test
    void diagnosticsExposeStoresAndAnnotationMetadata() {
        InMemoryStore explicitBackingStore = new InMemoryStore("explicitBacking", "primary");
        InMemoryCacheStore explicitCacheStore = new InMemoryCacheStore("explicitCache");
        EntityStoreRegistry registry = new DefaultEntityStoreRegistry(
                List.of(explicitBackingStore, explicitCacheStore),
                new EmptyEntityStoreRegistry());
        KinexisDiagnosticsService diagnosticsService = new KinexisDiagnosticsService(
                List.of(explicitBackingStore, explicitCacheStore),
                List.of(processor),
                List.of(),
                List.of(),
                registry,
                new AnnotationFinder());

        KinexisDiagnosticsService.EntityDiagnostics diagnostics = diagnosticsService.entity(TestEntity.class);

        assertTrue(diagnostics.annotated());
        assertTrue(diagnostics.enabled());
        assertEquals(Set.of(CachingPattern.CACHE_ASIDE), diagnostics.patterns());
        assertEquals(5, diagnostics.ttl());
        assertEquals("explicitCache", diagnostics.cacheStore().orElseThrow().name());
        assertEquals("explicitBacking", diagnostics.primaryStore().orElseThrow().name());
        assertEquals(List.of("explicitBacking"), diagnostics.targetStores().stream().map(KinexisDiagnosticsService.StoreDiagnostics::name).toList());
    }

    @Test
    void diagnosticsIncludeGeneratedEntityRegistries() {
        KinexisEntityRegistry entityRegistry = () -> Set.of(RegistryOnlyEntity.class);
        KinexisDiagnosticsService diagnosticsService = new KinexisDiagnosticsService(
                List.of(),
                List.of(),
                List.of(),
                List.of(entityRegistry),
                new EmptyEntityStoreRegistry(),
                new AnnotationFinder());

        assertEquals(List.of(RegistryOnlyEntity.class), diagnosticsService.stores().stream()
                .map(KinexisDiagnosticsService.EntityDiagnostics::entityType)
                .toList());
    }

    @Test
    void storeValidatorReportsMissingStoresAndRefreshAheadWarning() {
        RefreshAheadService service = new RefreshAheadService();
        KinexisDiagnosticsService diagnosticsService = new KinexisDiagnosticsService(
                List.of(),
                List.of(),
                List.of(service),
                List.of(),
                new EmptyEntityStoreRegistry(),
                new AnnotationFinder());
        KinexisStoreValidator validator = new KinexisStoreValidator(diagnosticsService, new KinexisProperties());

        KinexisStoreValidator.ValidationResult result = validator.validate();

        assertTrue(result.errors().stream().anyMatch(error -> error.contains("no CacheStore")));
        assertTrue(result.errors().stream().anyMatch(error -> error.contains("no primary EntityStore")));
        assertTrue(result.warnings().stream().anyMatch(warning -> warning.contains("ttl is not positive")));
    }

    @Test
    void storeValidatorReportsDuplicateStoresAmbiguousTargetsInvalidParallelismAndRepositoryDiscovery() {
        KinexisProperties properties = new KinexisProperties();
        properties.getProcessing().setMaxParallelStores(0);
        properties.getStores().getRepositoryDiscovery().setEnabled(true);
        InMemoryStore firstStore = new InMemoryStore("duplicate", "shared");
        InMemoryStore duplicateStore = new InMemoryStore("duplicate", "other");
        InMemoryStore ambiguousTargetStore = new InMemoryStore("otherStore", "shared");
        KinexisDiagnosticsService diagnosticsService = new KinexisDiagnosticsService(
                List.of(firstStore, duplicateStore, ambiguousTargetStore),
                List.of(),
                List.of(),
                List.of(),
                new DefaultEntityStoreRegistry(
                        List.of(firstStore, duplicateStore, ambiguousTargetStore),
                        new EmptyEntityStoreRegistry()),
                new AnnotationFinder());
        KinexisStoreValidator validator = new KinexisStoreValidator(diagnosticsService, properties);

        KinexisStoreValidator.ValidationResult result = validator.validate();

        assertTrue(result.errors().stream().anyMatch(error -> error.contains("max-parallel-stores")));
        assertTrue(result.errors().stream().anyMatch(error -> error.contains("duplicate store name 'duplicate'")));
        assertTrue(result.errors().stream().anyMatch(error -> error.contains("ambiguous target alias 'shared'")));
        assertTrue(result.warnings().stream().anyMatch(warning -> warning.contains("repository-discovery.enabled is true")));
    }

    @Test
    void serviceRejectsUnknownWriteBehindTargetBeforePublishing() throws Exception {
        WriteBehindService service = new WriteBehindService();
        CountingEventPublisher publisher = new CountingEventPublisher();
        EntityStoreRegistry registry = new WriteBehindRegistry(List.of(new WriteBehindStore("primaryStore", "primary")));
        injectService(service, registry, publisher);

        assertThrows(IllegalArgumentException.class, () -> service.save(new WriteBehindEntity(91L, "target"), "missing"));
        assertEquals(0, publisher.appendCount);
    }

    @Test
    void redisOmCacheStoreAppliesRedisTtlToResolvedEntityKey() {
        BeanFinder beanFinder = new BeanFinder(new StaticListableBeanFactory());
        InMemoryCrudRepository repository = new InMemoryCrudRepository();
        RedisOmCacheStore<TestEntity> redisOmStore = RedisOmCacheStore
                .builder(TestEntity.class, repository, beanFinder)
                .name("redisOmEmployerCache")
                .redisTemplate(redisTemplate)
                .build();
        String redisKey = Misc.getEntityKeyPrefix(TestEntity.class) + Misc.KEY_SEPARATOR + 41L;
        redisTemplate.opsForValue().set(redisKey, "cached");

        redisOmStore.save(new TestEntity(41L, "TTL"), Duration.ofSeconds(30));

        Long ttl = redisTemplate.getExpire(redisKey);
        assertNotNull(ttl);
        assertTrue(ttl > 0 && ttl <= 30);
    }

    @Test
    void streamLifecycleCreatesConsumerGroupIdempotently() {
        KinexisStreamLifecycle lifecycle = new KinexisStreamLifecycle(redisTemplate, null);
        String streamKey = Misc.getStreamKey(TestEntity.class);
        String group = "lifecycle_group";

        lifecycle.ensureConsumerGroup(streamKey, group);
        lifecycle.ensureConsumerGroup(streamKey, group);

        PendingMessagesSummary pending = redisTemplate.opsForStream().pending(streamKey, group);
        assertNotNull(pending);
        assertEquals(0, pending.getTotalPendingMessages());
    }

    @Test
    void pendingHandlerUsesTypedPendingConfiguration() {
        KinexisProperties properties = new KinexisProperties();
        properties.getStream().getListener().getPending().setMaxAttempts(7);
        properties.getStream().getListener().getPending().setMaxRetention(Duration.ofSeconds(9).toMillis());
        properties.getStream().getListener().getPending().setBatchSize(13);
        TestPendingMessageHandler handler = new TestPendingMessageHandler(processor);

        handler.setKinexisProperties(properties);

        assertEquals(7, handler.maxAttempts());
        assertEquals(Duration.ofSeconds(9).toMillis(), handler.maxRetention());
        assertEquals(13, handler.batchSize());
    }

    @Test
    void serviceAppliesAnnotationTtlWhenWritingToCache() throws Exception {
        TestService service = new TestService();
        injectService(service, new TestStoreRegistry(cacheStore, backingStore), new CountingEventPublisher());
        TestEntity entity = new TestEntity(21L, "TTL");

        service.save(entity);

        assertEquals(Optional.of(entity), cacheStore.findById(21L));
        assertEquals(Duration.ofSeconds(5), cacheStore.lastTtl);
        assertTrue(backingStore.findById(21L).isEmpty());
    }

    @Test
    void disabledAnnotationBypassesCacheAndStreams() throws Exception {
        DisabledStore primary = new DisabledStore("primary");
        DisabledCacheStore cache = new DisabledCacheStore("cache");
        DisabledEntity primaryEntity = new DisabledEntity(31L, "primary");
        DisabledEntity cacheEntity = new DisabledEntity(31L, "cache");
        primary.save(primaryEntity);
        cache.save(cacheEntity);
        DisabledService service = new DisabledService();
        CountingEventPublisher publisher = new CountingEventPublisher();
        injectService(service, new DisabledRegistry(primary, cache), publisher);

        Optional<DisabledEntity> found = service.findById(31L);
        service.save(new DisabledEntity(32L, "saved"));
        service.delete(31L);

        assertEquals(Optional.of(primaryEntity), found);
        assertEquals(Optional.of(new DisabledEntity(32L, "saved")), primary.findById(32L));
        assertTrue(primary.findById(31L).isEmpty());
        assertEquals(Optional.of(cacheEntity), cache.findById(31L));
        assertEquals(0, publisher.appendCount);
    }

    @Test
    void processorWritesOnlyToSelectedTargetsWhenEventHasTargets() throws Exception {
        InMemoryStore mysqlStore = new InMemoryStore("mysqlStore", "mysql");
        InMemoryStore mongoStore = new InMemoryStore("mongoStore", "mongo");
        inject(processor, "entityStoreRegistry", new DefaultEntityStoreRegistry(
                List.of(mysqlStore, mongoStore),
                new EmptyEntityStoreRegistry()));
        MapRecord<String, String, String> record = enqueueAndRead(KinexisEvent.save(
                TestEntity.class,
                objectMapper.writeValueAsString(new TestEntity(7L, "Targeted")),
                "mongo"));

        processor.process(record);
        processor.acknowledge(record);

        assertTrue(mysqlStore.findById(7L).isEmpty());
        assertEquals(Optional.of(new TestEntity(7L, "Targeted")), mongoStore.findById(7L));
        assertEquals(0, pendingCount());
    }

    @Test
    void processorFansOutStoreWritesConcurrently() throws Exception {
        CountDownLatch entered = new CountDownLatch(2);
        BlockingStore firstStore = new BlockingStore("firstStore", entered);
        BlockingStore secondStore = new BlockingStore("secondStore", entered);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            inject(processor, "entityStoreRegistry", new DefaultEntityStoreRegistry(
                    List.of(firstStore, secondStore),
                    new EmptyEntityStoreRegistry()));
            inject(processor, "storeExecutor", executor);
            MapRecord<String, String, String> record = enqueueAndRead(KinexisEvent.save(
                    TestEntity.class,
                    objectMapper.writeValueAsString(new TestEntity(12L, "Parallel"))));

            processor.process(record);

            assertEquals(Optional.of(new TestEntity(12L, "Parallel")), firstStore.findById(12L));
            assertEquals(Optional.of(new TestEntity(12L, "Parallel")), secondStore.findById(12L));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void entityLevelProcessorWritesSelectedTargetsWithoutRepositorySpecificProcessor() throws Exception {
        EntityLevelProcessor entityProcessor = new EntityLevelProcessor(redisTemplate, objectMapper);
        InMemoryStore mysqlStore = new InMemoryStore("mysqlStore", "mysql");
        InMemoryStore mongoStore = new InMemoryStore("mongoStore", "mongo");
        inject(entityProcessor, "entityStoreRegistry", new DefaultEntityStoreRegistry(
                List.of(mysqlStore, mongoStore),
                new EmptyEntityStoreRegistry()));
        MapRecord<String, String, String> record = enqueueAndRead(
                KinexisEvent.save(
                        TestEntity.class,
                        objectMapper.writeValueAsString(new TestEntity(11L, "EntityLevel")),
                        "mongo"),
                TestEntity.class);

        entityProcessor.process(record);
        entityProcessor.acknowledge(record);

        assertTrue(mysqlStore.findById(11L).isEmpty());
        assertEquals(Optional.of(new TestEntity(11L, "EntityLevel")), mongoStore.findById(11L));
        assertEquals(0, pendingCount(TestEntity.class));
    }

    @Test
    void unknownTargetFailsProcessingAndLeavesMessagePending() throws Exception {
        InMemoryStore primaryStore = new InMemoryStore("primaryStore", "primary");
        inject(processor, "entityStoreRegistry", new DefaultEntityStoreRegistry(
                List.of(primaryStore),
                new EmptyEntityStoreRegistry()));
        MapRecord<String, String, String> record = enqueueAndRead(KinexisEvent.save(
                TestEntity.class,
                objectMapper.writeValueAsString(new TestEntity(9L, "Unknown")),
                "missing"));

        assertThrows(ProcessMessageException.class, () -> processor.process(record));

        assertTrue(primaryStore.findById(9L).isEmpty());
        assertEquals(1, pendingCount());
    }

    @Test
    void processorDeletesEntityAndAcknowledgesAfterSuccess() throws Exception {
        backingStore.save(new TestEntity(3L, "Linus"));
        MapRecord<String, String, String> record = enqueueAndRead(KinexisEvent.delete(TestEntity.class, 3L));

        processor.process(record);
        processor.acknowledge(record);

        assertTrue(backingStore.findById(3L).isEmpty());
        assertEquals(0, pendingCount());
    }

    @Test
    void failedProcessingLeavesMessagePendingForRetry() throws Exception {
        backingStore.failSaves = true;
        MapRecord<String, String, String> record = enqueueAndRead(KinexisEvent.save(TestEntity.class, objectMapper.writeValueAsString(new TestEntity(4L, "Fail"))));

        assertThrows(ProcessMessageException.class, () -> processor.process(record));

        assertEquals(1, pendingCount());
    }

    @Test
    void pendingHandlerRetriesAndAcknowledgesSuccessfulPendingMessage() throws Exception {
        enqueueAndRead(KinexisEvent.save(TestEntity.class, objectMapper.writeValueAsString(new TestEntity(5L, "Retry"))));
        TestPendingMessageHandler handler = pendingHandler(processor, 3);

        handler.processPendingMessages();

        assertEquals(Optional.of(new TestEntity(5L, "Retry")), backingStore.findById(5L));
        assertEquals(0, pendingCount());
    }

    @Test
    void pendingHandlerMovesFailedMessageToDeadLetterStream() throws Exception {
        backingStore.failSaves = true;
        enqueueAndRead(KinexisEvent.save(TestEntity.class, objectMapper.writeValueAsString(new TestEntity(6L, "DLQ"))));
        TestPendingMessageHandler handler = pendingHandler(processor, 1);

        handler.processPendingMessages();

        assertEquals(0, pendingCount());
        List<MapRecord<String, Object, Object>> dlqRecords = redisTemplate.opsForStream()
                .range(Misc.getDLQStreamKey(TestEntity.class), Range.unbounded());
        assertNotNull(dlqRecords);
        assertEquals(1, dlqRecords.size());
        assertEquals("Too many attempts", dlqRecords.getFirst().getValue().get("reason"));
        assertEquals("1", dlqRecords.getFirst().getValue().get(AbstractPendingMessageHandler.DLQ_ATTEMPTS_KEY));
        assertEquals("backing", dlqRecords.getFirst().getValue().get(AbstractPendingMessageHandler.DLQ_FAILED_STORE_KEY));
        assertEquals(ProcessMessageException.class.getName(), dlqRecords.getFirst().getValue().get(AbstractPendingMessageHandler.DLQ_EXCEPTION_CLASS_KEY));
        assertNotNull(dlqRecords.getFirst().getValue().get(AbstractPendingMessageHandler.DLQ_FAILURE_TIMESTAMP_KEY));
    }

    @Test
    void dlqServiceReplaysDeadLetterMessageToOriginalStream() throws Exception {
        backingStore.failSaves = true;
        enqueueAndRead(KinexisEvent.save(TestEntity.class, objectMapper.writeValueAsString(new TestEntity(8L, "Replay"))));
        TestPendingMessageHandler handler = pendingHandler(processor, 1);
        handler.processPendingMessages();
        List<MapRecord<String, Object, Object>> dlqRecords = redisTemplate.opsForStream()
                .range(Misc.getDLQStreamKey(TestEntity.class), Range.unbounded());
        assertNotNull(dlqRecords);
        assertEquals(1, dlqRecords.size());

        KinexisDlqService dlqService = new KinexisDlqService(redisTemplate);
        Optional<String> replayedId = dlqService.replay(TestEntity.class, dlqRecords.getFirst().getId().getValue());

        assertTrue(replayedId.isPresent());
        List<MapRecord<String, Object, Object>> streamRecords = redisTemplate.opsForStream()
                .range(Misc.getStreamKey(TestEntity.class), Range.unbounded());
        assertNotNull(streamRecords);
        assertEquals(2, streamRecords.size());
        assertFalse(streamRecords.getLast().getValue().containsKey(AbstractPendingMessageHandler.DLQ_REASON_KEY));
    }

    @Test
    void dlqServiceCanReplayToSpecificTargetsAndDeleteOriginalDlqRecord() throws Exception {
        backingStore.failSaves = true;
        enqueueAndRead(KinexisEvent.save(TestEntity.class, objectMapper.writeValueAsString(new TestEntity(10L, "ReplayTarget"))));
        TestPendingMessageHandler handler = pendingHandler(processor, 1);
        handler.processPendingMessages();
        List<MapRecord<String, Object, Object>> dlqRecords = redisTemplate.opsForStream()
                .range(Misc.getDLQStreamKey(TestEntity.class), Range.unbounded());
        assertNotNull(dlqRecords);
        assertEquals(1, dlqRecords.size());

        KinexisDlqService dlqService = new KinexisDlqService(redisTemplate);
        Optional<String> replayedId = dlqService.replay(
                TestEntity.class,
                dlqRecords.getFirst().getId().getValue(),
                KinexisDlqService.ReplayMode.REPLAY_AND_DELETE,
                "mongo", "primary");

        assertTrue(replayedId.isPresent());
        List<MapRecord<String, Object, Object>> streamRecords = redisTemplate.opsForStream()
                .range(Misc.getStreamKey(TestEntity.class), Range.unbounded());
        assertNotNull(streamRecords);
        assertEquals("mongo,primary", streamRecords.getLast().getValue().get(KinexisEvent.EVENT_TARGETS_KEY));
        List<MapRecord<String, Object, Object>> remainingDlqRecords = redisTemplate.opsForStream()
                .range(Misc.getDLQStreamKey(TestEntity.class), Range.unbounded());
        assertNotNull(remainingDlqRecords);
        assertTrue(remainingDlqRecords.isEmpty());
    }

    @Test
    void dlqServiceRejectsMalformedDlqRecords() {
        String dlqStreamKey = Misc.getDLQStreamKey(TestEntity.class);
        RecordId malformedId = redisTemplate.opsForStream().add(StreamRecords.newRecord()
                .withId(RecordId.autoGenerate())
                .ofMap(Map.of(AbstractPendingMessageHandler.DLQ_REASON_KEY, "manual"))
                .withStreamKey(dlqStreamKey));
        assertNotNull(malformedId);
        KinexisDlqService dlqService = new KinexisDlqService(redisTemplate);

        assertThrows(IllegalArgumentException.class, () -> dlqService.replay(TestEntity.class, malformedId.getValue()));
    }

    private TestPendingMessageHandler pendingHandler(TestProcessor processor, int maxAttempts) throws Exception {
        TestPendingMessageHandler handler = new TestPendingMessageHandler(processor);
        inject(handler, "redisTemplate", redisTemplate);
        handler.configure(maxAttempts, Duration.ofSeconds(30).toMillis(), 10);
        return handler;
    }

    private MapRecord<String, String, String> enqueueAndRead(KinexisEvent event) {
        return enqueueAndRead(event, TestEntity.class);
    }

    private MapRecord<String, String, String> enqueueAndRead(KinexisEvent event, Class<?> consumerGroupType) {
        String streamKey = Misc.getStreamKey(TestEntity.class);
        String group = Misc.getConsumerGroup(consumerGroupType);
        String consumer = Misc.getConsumerName(consumerGroupType);
        redisTemplate.opsForStream().add(StreamRecords.newRecord()
                .withId(RecordId.autoGenerate())
                .ofMap(event.toRecordMap())
                .withStreamKey(streamKey));
        redisTemplate.opsForStream().createGroup(streamKey, ReadOffset.from("0"), group);
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                Consumer.from(group, consumer),
                StreamReadOptions.empty().count(1),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()));
        assertNotNull(records);
        assertEquals(1, records.size());
        return convertRecord(records.getFirst());
    }

    private MapRecord<String, String, String> convertRecord(MapRecord<String, Object, Object> record) {
        Map<String, String> values = new java.util.HashMap<>();
        record.getValue().forEach((key, value) -> values.put(String.valueOf(key), String.valueOf(value)));
        return StreamRecords.newRecord()
                .withId(record.getId())
                .ofMap(values)
                .withStreamKey(record.getStream());
    }

    private long pendingCount() {
        return pendingCount(TestEntity.class);
    }

    private long pendingCount(Class<?> consumerGroupType) {
        PendingMessagesSummary pending = redisTemplate.opsForStream()
                .pending(Misc.getStreamKey(TestEntity.class), Misc.getConsumerGroup(consumerGroupType));
        return pending == null ? 0 : pending.getTotalPendingMessages();
    }

    private static void inject(Object target, String fieldName, Object value) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void injectService(Object service, EntityStoreRegistry registry, EventPublisher eventPublisher) throws Exception {
        inject(service, "objectMapper", new ObjectMapper());
        inject(service, "annotationFinder", new AnnotationFinder());
        inject(service, "entityStoreRegistry", registry);
        inject(service, "eventPublisher", eventPublisher);
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

    private static final class TestProcessor extends AbstractProcessor<TestEntity> {

        private final RedisTemplate<String, String> redisTemplate;
        private final ObjectMapper objectMapper;

        private TestProcessor(RedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {
            this.redisTemplate = redisTemplate;
            this.objectMapper = objectMapper;
        }

        @Override
        public RedisTemplate<String, String> getRedisTemplate() {
            return redisTemplate;
        }

        @Override
        public ObjectMapper getObjectMapper() {
            return objectMapper;
        }
    }

    private static final class EntityLevelProcessor extends AbstractProcessor<TestEntity> {

        private final RedisTemplate<String, String> redisTemplate;
        private final ObjectMapper objectMapper;

        private EntityLevelProcessor(RedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {
            this.redisTemplate = redisTemplate;
            this.objectMapper = objectMapper;
        }

        @Override
        public RedisTemplate<String, String> getRedisTemplate() {
            return redisTemplate;
        }

        @Override
        public ObjectMapper getObjectMapper() {
            return objectMapper;
        }
    }

    private static final class TestPendingMessageHandler extends AbstractPendingMessageHandler<TestEntity> {

        private final TestProcessor processor;

        private TestPendingMessageHandler(TestProcessor processor) {
            this.processor = processor;
        }

        @Override
        public TestProcessor getProcessor() {
            return processor;
        }

        private void configure(int maxAttempts, long maxRetention, int batchSize) {
            MAX_ATTEMPTS = maxAttempts;
            MAX_RETENTION = maxRetention;
            BATCH_SIZE = batchSize;
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

    private static final class TestService extends KinexisService<TestEntity> {
    }

    private static final class DisabledService extends KinexisService<DisabledEntity> {
    }

    private static final class RefreshAheadService extends KinexisService<RefreshAheadEntity> {
    }

    private static final class WriteBehindService extends KinexisService<WriteBehindEntity> {
    }

    private static final class CountingEventPublisher implements EventPublisher {

        private int appendCount;

        @Override
        public String append(Class<?> entityType, KinexisEvent event) {
            appendCount++;
            return "test-record";
        }
    }

    private static final class TestStoreRegistry implements EntityStoreRegistry {

        private final InMemoryCacheStore cacheStore;
        private final InMemoryStore backingStore;

        private TestStoreRegistry(InMemoryCacheStore cacheStore, InMemoryStore backingStore) {
            this.cacheStore = cacheStore;
            this.backingStore = backingStore;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<CacheStore<T>> findCacheStore(Class<T> entityType) {
            return Optional.of((CacheStore<T>) cacheStore);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<EntityStore<T>> findPrimaryStore(Class<T> entityType) {
            return Optional.of((EntityStore<T>) backingStore);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> List<EntityStore<T>> findTargetStores(Class<T> entityType) {
            return List.of((EntityStore<T>) backingStore);
        }
    }

    private static class InMemoryStore implements EntityStore<TestEntity> {

        private final String name;
        private final Set<String> targets;
        private final Map<Object, TestEntity> entities = new ConcurrentHashMap<>();
        private boolean failSaves;

        private InMemoryStore(String name) {
            this(name, name);
        }

        private InMemoryStore(String name, String... targets) {
            this.name = name;
            this.targets = Set.of(targets);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<TestEntity> entityType() {
            return TestEntity.class;
        }

        @Override
        public Set<String> targets() {
            return targets;
        }

        @Override
        public Optional<TestEntity> findById(Object id) {
            return Optional.ofNullable(entities.get(normalizeId(id)));
        }

        @Override
        public TestEntity save(TestEntity entity) {
            if (failSaves) {
                throw new IllegalStateException("store failure");
            }
            entities.put(entity.id(), entity);
            return entity;
        }

        @Override
        public void deleteById(Object id) {
            entities.remove(normalizeId(id));
        }

        private Object normalizeId(Object id) {
            if (id instanceof String value) {
                return Long.valueOf(value);
            }
            return id;
        }
    }

    private static final class InMemoryCacheStore extends InMemoryStore implements CacheStore<TestEntity> {

        private Duration lastTtl = Duration.ZERO;

        private InMemoryCacheStore(String name) {
            super(name);
        }

        @Override
        public TestEntity save(TestEntity entity, Duration ttl) {
            lastTtl = ttl;
            return save(entity);
        }
    }

    private static final class BlockingStore extends InMemoryStore {

        private final CountDownLatch entered;

        private BlockingStore(String name, CountDownLatch entered) {
            super(name);
            this.entered = entered;
        }

        @Override
        public TestEntity save(TestEntity entity) {
            entered.countDown();
            try {
                if (!entered.await(2, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting for concurrent store fan-out");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for concurrent store fan-out", e);
            }
            return super.save(entity);
        }
    }

    @CachingPatterns(patterns = {CachingPattern.CACHE_ASIDE}, ttl = 5)
    private record TestEntity(Long id, String name) {
    }

    @CachingPatterns(patterns = {CachingPattern.WRITE_BEHIND, CachingPattern.CACHE_ASIDE}, enabled = false, ttl = 5)
    private record DisabledEntity(Long id, String name) {
    }

    @CachingPatterns(patterns = {CachingPattern.REFRESH_AHEAD})
    private record RefreshAheadEntity(Long id, String name) {
    }

    @CachingPatterns(patterns = {CachingPattern.WRITE_BEHIND})
    private record WriteBehindEntity(Long id, String name) {
    }

    @CachingPatterns(patterns = {CachingPattern.NONE})
    private record RegistryOnlyEntity(Long id, String name) {
    }

    private static final class TestRepository {
    }

    private static final class InMemoryCrudRepository implements CrudRepository<TestEntity, Long> {

        private final Map<Long, TestEntity> entities = new ConcurrentHashMap<>();

        @Override
        public <S extends TestEntity> S save(S entity) {
            entities.put(entity.id(), entity);
            return entity;
        }

        @Override
        public <S extends TestEntity> Iterable<S> saveAll(Iterable<S> entities) {
            entities.forEach(this::save);
            return entities;
        }

        @Override
        public Optional<TestEntity> findById(Long id) {
            return Optional.ofNullable(entities.get(id));
        }

        @Override
        public boolean existsById(Long id) {
            return entities.containsKey(id);
        }

        @Override
        public Iterable<TestEntity> findAll() {
            return entities.values();
        }

        @Override
        public Iterable<TestEntity> findAllById(Iterable<Long> ids) {
            java.util.ArrayList<TestEntity> found = new java.util.ArrayList<>();
            ids.forEach(id -> {
                TestEntity entity = entities.get(id);
                if (entity != null) {
                    found.add(entity);
                }
            });
            return found;
        }

        @Override
        public long count() {
            return entities.size();
        }

        @Override
        public void deleteById(Long id) {
            entities.remove(id);
        }

        @Override
        public void delete(TestEntity entity) {
            entities.remove(entity.id());
        }

        @Override
        public void deleteAllById(Iterable<? extends Long> ids) {
            ids.forEach(entities::remove);
        }

        @Override
        public void deleteAll(Iterable<? extends TestEntity> entities) {
            entities.forEach(this::delete);
        }

        @Override
        public void deleteAll() {
            entities.clear();
        }
    }

    private static final class DisabledRegistry implements EntityStoreRegistry {

        private final DisabledStore primary;
        private final DisabledCacheStore cache;

        private DisabledRegistry(DisabledStore primary, DisabledCacheStore cache) {
            this.primary = primary;
            this.cache = cache;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<CacheStore<T>> findCacheStore(Class<T> entityType) {
            return Optional.of((CacheStore<T>) cache);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Optional<EntityStore<T>> findPrimaryStore(Class<T> entityType) {
            return Optional.of((EntityStore<T>) primary);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> List<EntityStore<T>> findTargetStores(Class<T> entityType) {
            return List.of((EntityStore<T>) primary);
        }
    }

    private static final class WriteBehindRegistry implements EntityStoreRegistry {

        private final List<EntityStore<WriteBehindEntity>> stores;

        private WriteBehindRegistry(List<EntityStore<WriteBehindEntity>> stores) {
            this.stores = stores;
        }

        @Override
        public <T> Optional<CacheStore<T>> findCacheStore(Class<T> entityType) {
            return Optional.empty();
        }

        @Override
        public <T> Optional<EntityStore<T>> findPrimaryStore(Class<T> entityType) {
            return Optional.empty();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> List<EntityStore<T>> findTargetStores(Class<T> entityType) {
            return entityType.equals(WriteBehindEntity.class) ? (List<EntityStore<T>>) (List<?>) stores : List.of();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> List<EntityStore<T>> findTargetStores(Class<T> entityType, java.util.Collection<String> targets) {
            if (!entityType.equals(WriteBehindEntity.class)) {
                return List.of();
            }
            return (List<EntityStore<T>>) (List<?>) stores.stream()
                    .filter(store -> store.targets().stream().anyMatch(targets::contains))
                    .toList();
        }
    }

    private static final class WriteBehindStore implements EntityStore<WriteBehindEntity> {

        private final String name;
        private final Set<String> targets;

        private WriteBehindStore(String name, String... targets) {
            this.name = name;
            this.targets = Set.of(targets);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<WriteBehindEntity> entityType() {
            return WriteBehindEntity.class;
        }

        @Override
        public Set<String> targets() {
            return targets;
        }

        @Override
        public Optional<WriteBehindEntity> findById(Object id) {
            return Optional.empty();
        }

        @Override
        public WriteBehindEntity save(WriteBehindEntity entity) {
            return entity;
        }

        @Override
        public void deleteById(Object id) {
        }
    }

    private static class DisabledStore implements EntityStore<DisabledEntity> {

        private final String name;
        private final Map<Object, DisabledEntity> entities = new ConcurrentHashMap<>();

        private DisabledStore(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Class<DisabledEntity> entityType() {
            return DisabledEntity.class;
        }

        @Override
        public Optional<DisabledEntity> findById(Object id) {
            return Optional.ofNullable(entities.get(normalizeId(id)));
        }

        @Override
        public DisabledEntity save(DisabledEntity entity) {
            entities.put(entity.id(), entity);
            return entity;
        }

        @Override
        public void deleteById(Object id) {
            entities.remove(normalizeId(id));
        }

        private Object normalizeId(Object id) {
            if (id instanceof String value) {
                return Long.valueOf(value);
            }
            return id;
        }
    }

    private static final class DisabledCacheStore extends DisabledStore implements CacheStore<DisabledEntity> {

        private DisabledCacheStore(String name) {
            super(name);
        }
    }
}
