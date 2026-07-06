package com.foogaro.kinexis.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foogaro.kinexis.core.annotation.CachingPatterns;
import com.foogaro.kinexis.core.config.KinexisProperties;
import com.foogaro.kinexis.core.exception.AcknowledgeMessageException;
import com.foogaro.kinexis.core.exception.KinexisBackpressureException;
import com.foogaro.kinexis.core.exception.ProcessMessageException;
import com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler;
import com.foogaro.kinexis.core.handler.KinexisDlqWriter;
import com.foogaro.kinexis.core.listener.AbstractStreamListener;
import com.foogaro.kinexis.core.model.CachingPattern;
import com.foogaro.kinexis.core.model.KinexisDlqRecord;
import com.foogaro.kinexis.core.model.KinexisEvent;
import com.foogaro.kinexis.core.processor.AbstractProcessor;
import com.foogaro.kinexis.core.processor.KinexisProcessingCoordinator;
import com.foogaro.kinexis.core.processor.KinexisProcessingMetrics;
import com.foogaro.kinexis.core.processor.KinexisStoreExecutor;
import com.foogaro.kinexis.core.processor.Processor;
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
import com.foogaro.kinexis.core.stream.StreamPartitioner;
import com.foogaro.kinexis.core.telemetry.KinexisTelemetry;
import com.foogaro.kinexis.core.telemetry.KinexisTelemetrySnapshot;
import com.foogaro.kinexis.core.telemetry.SimpleKinexisTelemetry;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
        assertNotNull(records.getFirst().getValue().get(KinexisEvent.EVENT_ID_KEY));
    }

    @Test
    void telemetryRecordsPublishedProcessedStoreLatencyAndStoreFailures() throws Exception {
        SimpleKinexisTelemetry telemetry = new SimpleKinexisTelemetry();
        RedisStreamEventPublisher publisher = new RedisStreamEventPublisher(
                redisTemplate,
                new StreamPartitioner(new KinexisProperties()),
                telemetry);
        TestEntity entity = new TestEntity(61L, "Telemetry");
        KinexisEvent event = KinexisEvent.save(TestEntity.class, 61L, objectMapper.writeValueAsString(entity));

        publisher.append(TestEntity.class, event);
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream()
                .range(Misc.getStreamKey(TestEntity.class), Range.unbounded());
        assertNotNull(records);
        MapRecord<String, String, String> record = convertRecord(records.getFirst());
        inject(processor, "telemetry", telemetry);
        processor.process(record);

        KinexisTelemetrySnapshot successSnapshot = telemetry.snapshot();
        assertEquals(1, counter(successSnapshot, KinexisTelemetry.STREAM_EVENTS_PUBLISHED, "operation", "SAVE"));
        assertEquals(1, counter(successSnapshot, KinexisTelemetry.STREAM_EVENTS_PROCESSED, "operation", "SAVE"));
        assertEquals(1, timerCount(successSnapshot, KinexisTelemetry.STORE_WRITE_LATENCY, "store", "backing"));

        backingStore.failSaves = true;
        MapRecord<String, String, String> failedRecord = streamRecord(KinexisEvent.save(
                TestEntity.class,
                62L,
                objectMapper.writeValueAsString(new TestEntity(62L, "Failure"))));

        assertThrows(ProcessMessageException.class, () -> processor.process(failedRecord));
        KinexisTelemetrySnapshot failureSnapshot = telemetry.snapshot();
        assertEquals(1, counter(failureSnapshot, KinexisTelemetry.STORE_FAILURES, "exception", IllegalStateException.class.getName()));
    }

    @Test
    void publisherRoutesSameEntityIdToSamePartitionedStream() throws Exception {
        KinexisProperties properties = new KinexisProperties();
        properties.getStream().setPartitions(4);
        StreamPartitioner streamPartitioner = new StreamPartitioner(properties);
        RedisStreamEventPublisher publisher = new RedisStreamEventPublisher(redisTemplate, streamPartitioner);
        KinexisEvent firstEvent = KinexisEvent.save(TestEntity.class, 11L,
                objectMapper.writeValueAsString(new TestEntity(11L, "Ada")));
        KinexisEvent secondEvent = KinexisEvent.save(TestEntity.class, 11L,
                objectMapper.writeValueAsString(new TestEntity(11L, "Lovelace")));

        publisher.append(TestEntity.class, firstEvent);
        publisher.append(TestEntity.class, secondEvent);

        String partitionStream = streamPartitioner.streamKey(TestEntity.class, firstEvent);
        assertEquals(partitionStream, streamPartitioner.streamKey(TestEntity.class, secondEvent));
        List<MapRecord<String, Object, Object>> partitionRecords = redisTemplate.opsForStream()
                .range(partitionStream, Range.unbounded());
        assertNotNull(partitionRecords);
        assertEquals(2, partitionRecords.size());
        assertEquals(1, streamPartitioner.streamKeys(TestEntity.class).stream()
                .map(redisTemplate.opsForStream()::size)
                .filter(size -> size != null && size > 0)
                .count());
    }

    @Test
    void pendingHandlerRetriesPartitionedStreams() throws Exception {
        KinexisProperties properties = new KinexisProperties();
        properties.getStream().setPartitions(4);
        StreamPartitioner streamPartitioner = new StreamPartitioner(properties);
        KinexisEvent event = KinexisEvent.save(TestEntity.class, 12L,
                objectMapper.writeValueAsString(new TestEntity(12L, "PartitionRetry")));
        MapRecord<String, String, String> record = enqueueAndRead(event, TestEntity.class, streamPartitioner);
        TestPendingMessageHandler handler = pendingHandler(processor, 3, streamPartitioner);

        handler.processPendingMessages();

        assertEquals(Optional.of(new TestEntity(12L, "PartitionRetry")), backingStore.findById(12L));
        assertEquals(0, pendingCount(record.getStream(), TestEntity.class));
    }

    @Test
    void dlqReplayPreservesOriginalPartitionedStream() throws Exception {
        KinexisProperties properties = new KinexisProperties();
        properties.getStream().setPartitions(4);
        StreamPartitioner streamPartitioner = new StreamPartitioner(properties);
        backingStore.failSaves = true;
        KinexisEvent event = KinexisEvent.save(TestEntity.class, 13L,
                objectMapper.writeValueAsString(new TestEntity(13L, "PartitionDlq")));
        MapRecord<String, String, String> record = enqueueAndRead(event, TestEntity.class, streamPartitioner);
        TestPendingMessageHandler handler = pendingHandler(processor, 1, streamPartitioner);
        handler.processPendingMessages();
        List<MapRecord<String, Object, Object>> dlqRecords = redisTemplate.opsForStream()
                .range(Misc.getDLQStreamKey(TestEntity.class), Range.unbounded());
        assertNotNull(dlqRecords);
        assertEquals(1, dlqRecords.size());
        assertEquals(record.getStream(), dlqRecords.getFirst().getValue().get(AbstractPendingMessageHandler.DLQ_STREAM_KEY));

        KinexisDlqService dlqService = new KinexisDlqService(redisTemplate);
        Optional<String> replayedId = dlqService.replay(TestEntity.class, dlqRecords.getFirst().getId().getValue());

        assertTrue(replayedId.isPresent());
        List<MapRecord<String, Object, Object>> partitionRecords = redisTemplate.opsForStream()
                .range(record.getStream(), Range.unbounded());
        assertNotNull(partitionRecords);
        assertEquals(3, partitionRecords.size());
        assertEquals(replayedId.orElseThrow(), partitionRecords.getLast().getId().getValue());
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
        assertEquals(List.of(entityStore), registry.findTargetStores(TestEntity.class, List.of("mysqlEmployerStore")));
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
        properties.getStream().setPartitions(0);
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
        assertTrue(result.errors().stream().anyMatch(error -> error.contains("stream.partitions")));
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
    void servicePublishesWriteBehindEventsWithStableEntityIdAndEventId() throws Exception {
        WriteBehindService service = new WriteBehindService();
        CountingEventPublisher publisher = new CountingEventPublisher();
        EntityStoreRegistry registry = new WriteBehindRegistry(List.of(new WriteBehindStore("primaryStore", "primary")));
        injectService(service, registry, publisher);

        service.save(new WriteBehindEntity(92L, "event"), "primary");

        assertEquals(1, publisher.appendCount);
        assertNotNull(publisher.lastEvent);
        assertNotNull(publisher.lastEvent.eventId());
        assertEquals(Optional.of("92"), publisher.lastEvent.entityId());
        assertEquals(List.of("primary"), publisher.lastEvent.targets());
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
    void boundedStoreExecutorRejectsWhenQueueIsFullAndRecordsMetrics() throws Exception {
        KinexisProperties properties = new KinexisProperties();
        properties.getProcessing().getBackpressure().setQueueFullBehavior(KinexisProperties.QueueFullBehavior.REJECT_TO_DLQ);
        KinexisProcessingMetrics metrics = new KinexisProcessingMetrics();
        CountDownLatch release = new CountDownLatch(1);
        KinexisStoreExecutor executor = new KinexisStoreExecutor(
                1,
                1,
                properties.getProcessing().getBackpressure(),
                metrics,
                Thread::new);
        try {
            executor.execute(awaitingTask(release));
            executor.execute(awaitingTask(release));

            assertThrows(RejectedExecutionException.class, () -> executor.execute(awaitingTask(release)));
            KinexisProcessingMetrics.Snapshot snapshot = metrics.snapshot();
            assertEquals(2, snapshot.storeTasksSubmitted());
            assertEquals(1, snapshot.backpressureRejections());
            assertEquals(1, snapshot.storeExecutorQueueDepth());
        } finally {
            release.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void processorRejectsWhenMaxInFlightPerStreamIsReached() throws Exception {
        KinexisProperties properties = new KinexisProperties();
        properties.getProcessing().getBackpressure().setMaxInFlightPerStream(1);
        properties.getProcessing().getBackpressure().setQueueFullBehavior(KinexisProperties.QueueFullBehavior.REJECT_TO_DLQ);
        KinexisProcessingMetrics metrics = new KinexisProcessingMetrics();
        ControlledStore store = new ControlledStore("controlledStore", 1);
        inject(processor, "entityStoreRegistry", new DefaultEntityStoreRegistry(
                List.of(store),
                new EmptyEntityStoreRegistry()));
        inject(processor, "processingMetrics", metrics);
        inject(processor, "processingCoordinator", new KinexisProcessingCoordinator(redisTemplate, properties, metrics));
        MapRecord<String, String, String> firstRecord = streamRecord(KinexisEvent.save(
                TestEntity.class,
                51L,
                objectMapper.writeValueAsString(new TestEntity(51L, "First"))));
        MapRecord<String, String, String> secondRecord = streamRecord(KinexisEvent.save(
                TestEntity.class,
                52L,
                objectMapper.writeValueAsString(new TestEntity(52L, "Second"))));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<Void> first = CompletableFuture.runAsync(() -> processUnchecked(firstRecord), executor);
            assertTrue(store.awaitEntered());

            ProcessMessageException failure = assertThrows(ProcessMessageException.class, () -> processor.process(secondRecord));
            assertTrue(hasCause(failure, KinexisBackpressureException.class));
            assertEquals(1, metrics.snapshot().backpressureRejections());
            store.release();
            first.join();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void streamListenerMovesBackpressureRejectedRecordDirectlyToDlq() throws Exception {
        KinexisProperties properties = new KinexisProperties();
        properties.getProcessing().getBackpressure().setQueueFullBehavior(KinexisProperties.QueueFullBehavior.REJECT_TO_DLQ);
        KinexisProcessingMetrics metrics = new KinexisProcessingMetrics();
        MapRecord<String, String, String> record = enqueueAndRead(KinexisEvent.save(
                TestEntity.class,
                53L,
                objectMapper.writeValueAsString(new TestEntity(53L, "Reject"))));
        RejectingStreamListener listener = new RejectingStreamListener(redisTemplate, objectMapper);
        inject(listener, "properties", properties);
        inject(listener, "dlqWriter", new KinexisDlqWriter(redisTemplate, metrics));

        listener.onMessage(record);

        assertEquals(0, pendingCount());
        List<MapRecord<String, Object, Object>> dlqRecords = redisTemplate.opsForStream()
                .range(Misc.getDLQStreamKey(TestEntity.class), Range.unbounded());
        assertNotNull(dlqRecords);
        assertEquals(1, dlqRecords.size());
        assertEquals("Backpressure rejected", dlqRecords.getFirst().getValue().get(AbstractPendingMessageHandler.DLQ_REASON_KEY));
        assertEquals(1, metrics.snapshot().deadLetteredRecords());
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
    void serviceRecordsCacheHitsAndMisses() throws Exception {
        SimpleKinexisTelemetry telemetry = new SimpleKinexisTelemetry();
        TestService service = new TestService();
        injectService(service, new TestStoreRegistry(cacheStore, backingStore), new CountingEventPublisher());
        inject(service, "telemetry", telemetry);
        cacheStore.save(new TestEntity(22L, "Hit"));

        assertEquals(Optional.of(new TestEntity(22L, "Hit")), service.findById(22L));
        assertTrue(service.findById(23L).isEmpty());

        KinexisTelemetrySnapshot snapshot = telemetry.snapshot();
        assertEquals(1, counter(snapshot, KinexisTelemetry.CACHE_HITS, "entity", "TestEntity"));
        assertEquals(1, counter(snapshot, KinexisTelemetry.CACHE_MISSES, "entity", "TestEntity"));
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
    void processorSkipsDuplicateEventStoreWrites() throws Exception {
        CountingStore countingStore = new CountingStore("countingStore");
        inject(processor, "entityStoreRegistry", new DefaultEntityStoreRegistry(
                List.of(countingStore),
                new EmptyEntityStoreRegistry()));
        MapRecord<String, String, String> record = enqueueAndRead(KinexisEvent.save(
                TestEntity.class,
                71L,
                objectMapper.writeValueAsString(new TestEntity(71L, "Once"))));

        processor.process(record);
        processor.process(record);

        assertEquals(1, countingStore.saveCount());
        assertEquals(Optional.of(new TestEntity(71L, "Once")), countingStore.findById(71L));
    }

    @Test
    void processorRetriesOnlyStoresThatFailedForAnAlreadySeenEvent() throws Exception {
        CountingStore successfulStore = new CountingStore("successfulStore");
        CountingStore retryStore = new CountingStore("retryStore");
        retryStore.failSaves = true;
        inject(processor, "entityStoreRegistry", new DefaultEntityStoreRegistry(
                List.of(successfulStore, retryStore),
                new EmptyEntityStoreRegistry()));
        MapRecord<String, String, String> record = enqueueAndRead(KinexisEvent.save(
                TestEntity.class,
                72L,
                objectMapper.writeValueAsString(new TestEntity(72L, "Partial"))));

        assertThrows(ProcessMessageException.class, () -> processor.process(record));
        retryStore.failSaves = false;
        processor.process(record);

        assertEquals(1, successfulStore.saveCount());
        assertEquals(1, retryStore.saveCount());
        assertEquals(Optional.of(new TestEntity(72L, "Partial")), successfulStore.findById(72L));
        assertEquals(Optional.of(new TestEntity(72L, "Partial")), retryStore.findById(72L));
    }

    @Test
    void processorSerializesConcurrentRecordsForTheSameEntityId() throws Exception {
        ControlledStore store = new ControlledStore("controlledStore", 1);
        inject(processor, "entityStoreRegistry", new DefaultEntityStoreRegistry(
                List.of(store),
                new EmptyEntityStoreRegistry()));
        MapRecord<String, String, String> firstRecord = streamRecord(KinexisEvent.save(
                TestEntity.class,
                81L,
                objectMapper.writeValueAsString(new TestEntity(81L, "First"))));
        MapRecord<String, String, String> secondRecord = streamRecord(KinexisEvent.save(
                TestEntity.class,
                81L,
                objectMapper.writeValueAsString(new TestEntity(81L, "Second"))));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<Void> first = CompletableFuture.runAsync(() -> processUnchecked(firstRecord), executor);
            assertTrue(store.awaitEntered());
            CompletableFuture<Void> second = CompletableFuture.runAsync(() -> processUnchecked(secondRecord), executor);
            Thread.sleep(100);

            assertEquals(1, store.maxConcurrent());
            store.release();
            first.join();
            second.join();
            assertEquals(1, store.maxConcurrent());
            assertEquals(2, store.saveCount());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void processorKeepsDifferentEntityIdsParallel() throws Exception {
        ControlledStore store = new ControlledStore("controlledStore", 2);
        inject(processor, "entityStoreRegistry", new DefaultEntityStoreRegistry(
                List.of(store),
                new EmptyEntityStoreRegistry()));
        MapRecord<String, String, String> firstRecord = streamRecord(KinexisEvent.save(
                TestEntity.class,
                82L,
                objectMapper.writeValueAsString(new TestEntity(82L, "First"))));
        MapRecord<String, String, String> secondRecord = streamRecord(KinexisEvent.save(
                TestEntity.class,
                83L,
                objectMapper.writeValueAsString(new TestEntity(83L, "Second"))));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<Void> first = CompletableFuture.runAsync(() -> processUnchecked(firstRecord), executor);
            CompletableFuture<Void> second = CompletableFuture.runAsync(() -> processUnchecked(secondRecord), executor);
            assertTrue(store.awaitEntered());

            assertEquals(2, store.maxConcurrent());
            store.release();
            first.join();
            second.join();
            assertEquals(2, store.saveCount());
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
    void pendingHandlerMovesEachFailedTargetStoreToItsOwnDeadLetterRecord() throws Exception {
        CountingStore postgresql = new CountingStore("employerPostgresStore", "postgresql");
        InMemoryStore mysql = new InMemoryStore("employerMysqlStore", "mysql");
        InMemoryStore sqlserver = new InMemoryStore("employerSqlServerStore", "sqlserver");
        mysql.failSaves = true;
        sqlserver.failSaves = true;
        inject(processor, "entityStoreRegistry", new DefaultEntityStoreRegistry(
                List.of(postgresql, mysql, sqlserver),
                new EmptyEntityStoreRegistry()));
        MapRecord<String, String, String> record = enqueueAndRead(KinexisEvent.save(
                TestEntity.class,
                objectMapper.writeValueAsString(new TestEntity(7L, "PartialFailure")),
                "postgresql", "mysql", "sqlserver"));

        ProcessMessageException failure = assertThrows(ProcessMessageException.class, () -> processor.process(record));
        assertEquals(List.of("mysql", "sqlserver"), failure.getFailedStores());
        assertEquals(Optional.of(new TestEntity(7L, "PartialFailure")), postgresql.findById(7L));
        assertEquals(1, postgresql.saveCount());
        assertEquals(1, pendingCount());

        TestPendingMessageHandler handler = pendingHandler(processor, 1);
        handler.processPendingMessages();

        assertEquals(0, pendingCount());
        assertEquals(1, postgresql.saveCount());
        List<MapRecord<String, Object, Object>> dlqRecords = redisTemplate.opsForStream()
                .range(Misc.getDLQStreamKey(TestEntity.class), Range.unbounded());
        assertNotNull(dlqRecords);
        assertEquals(2, dlqRecords.size());
        Map<Object, Map<Object, Object>> dlqByFailedStore = dlqRecords.stream()
                .collect(java.util.stream.Collectors.toMap(
                        dlq -> dlq.getValue().get(AbstractPendingMessageHandler.DLQ_FAILED_STORE_KEY),
                        MapRecord::getValue));
        assertEquals(Set.of("mysql", "sqlserver"), dlqByFailedStore.keySet());
        assertEquals("mysql", dlqByFailedStore.get("mysql").get(KinexisEvent.EVENT_TARGETS_KEY));
        assertEquals("sqlserver", dlqByFailedStore.get("sqlserver").get(KinexisEvent.EVENT_TARGETS_KEY));
        assertEquals("Too many attempts", dlqByFailedStore.get("mysql").get(AbstractPendingMessageHandler.DLQ_REASON_KEY));
        assertEquals("1", dlqByFailedStore.get("mysql").get(AbstractPendingMessageHandler.DLQ_ATTEMPTS_KEY));
        assertEquals(ProcessMessageException.class.getName(), dlqByFailedStore.get("sqlserver").get(AbstractPendingMessageHandler.DLQ_EXCEPTION_CLASS_KEY));
        assertNotNull(dlqByFailedStore.get("sqlserver").get(AbstractPendingMessageHandler.DLQ_FAILURE_TIMESTAMP_KEY));
    }

    @Test
    void dlqServiceListsAndReplaysFailedStoresIndependently() throws Exception {
        CountingStore postgresql = new CountingStore("employerPostgresStore", "postgresql");
        CountingStore mysql = new CountingStore("employerMysqlStore", "mysql");
        CountingStore sqlserver = new CountingStore("employerSqlServerStore", "sqlserver");
        mysql.failSaves = true;
        sqlserver.failSaves = true;
        inject(processor, "entityStoreRegistry", new DefaultEntityStoreRegistry(
                List.of(postgresql, mysql, sqlserver),
                new EmptyEntityStoreRegistry()));
        MapRecord<String, String, String> originalRecord = enqueueAndRead(KinexisEvent.save(
                TestEntity.class,
                15L,
                objectMapper.writeValueAsString(new TestEntity(15L, "IndependentReplay")),
                "postgresql", "mysql", "sqlserver"));

        assertThrows(ProcessMessageException.class, () -> processor.process(originalRecord));
        TestPendingMessageHandler handler = pendingHandler(processor, 1);
        handler.processPendingMessages();

        KinexisDlqService dlqService = new KinexisDlqService(redisTemplate);
        List<KinexisDlqRecord> dlqRecords = dlqService.list(TestEntity.class);
        assertEquals(2, dlqRecords.size());
        assertEquals(Set.of("mysql", "sqlserver"), dlqRecords.stream().map(KinexisDlqRecord::failedStore).collect(java.util.stream.Collectors.toSet()));
        assertEquals(1, dlqService.listByFailedStore(TestEntity.class, "mysql").size());
        assertEquals(2, dlqService.listByOperation(TestEntity.class, Misc.Operation.SAVE.getValue()).size());
        assertEquals(2, dlqService.listOlderThan(TestEntity.class, Duration.ZERO).size());
        assertEquals(1, dlqService.list(TestEntity.class, "sqlserver", Misc.Operation.SAVE.getValue(), Duration.ZERO).size());
        assertEquals(originalRecord.getValue().get(KinexisEvent.EVENT_ID_KEY), dlqRecords.getFirst().eventId());
        assertEquals(TestEntity.class.getName(), dlqRecords.getFirst().entityType());
        assertEquals("15", dlqRecords.getFirst().entityId());
        assertEquals(1, dlqRecords.getFirst().attempts());
        assertNotNull(dlqRecords.getFirst().failureTimestamp());

        mysql.failSaves = false;
        List<String> mysqlReplayIds = dlqService.replayByStore(TestEntity.class, "mysql");
        assertEquals(1, mysqlReplayIds.size());
        MapRecord<String, String, String> mysqlReplayRecord = streamRecordById(mysqlReplayIds.getFirst());
        assertEquals("mysql", mysqlReplayRecord.getValue().get(KinexisEvent.EVENT_TARGETS_KEY));
        assertEquals(originalRecord.getValue().get(KinexisEvent.EVENT_ID_KEY), mysqlReplayRecord.getValue().get(KinexisEvent.EVENT_ID_KEY));
        processor.process(mysqlReplayRecord);

        assertEquals(1, postgresql.saveCount());
        assertEquals(1, mysql.saveCount());
        assertEquals(0, sqlserver.saveCount());
        assertEquals(Optional.of(new TestEntity(15L, "IndependentReplay")), mysql.findById(15L));
        assertTrue(sqlserver.findById(15L).isEmpty());

        sqlserver.failSaves = false;
        KinexisDlqRecord sqlserverDlq = dlqService.listByFailedStore(TestEntity.class, "sqlserver").getFirst();
        Optional<String> sqlserverReplayId = dlqService.replayFailedStore(TestEntity.class, sqlserverDlq.recordId());
        assertTrue(sqlserverReplayId.isPresent());
        MapRecord<String, String, String> sqlserverReplayRecord = streamRecordById(sqlserverReplayId.get());
        assertEquals("sqlserver", sqlserverReplayRecord.getValue().get(KinexisEvent.EVENT_TARGETS_KEY));
        processor.process(sqlserverReplayRecord);

        assertEquals(1, postgresql.saveCount());
        assertEquals(1, mysql.saveCount());
        assertEquals(1, sqlserver.saveCount());
        assertEquals(Optional.of(new TestEntity(15L, "IndependentReplay")), sqlserver.findById(15L));
    }

    @Test
    void dlqServiceReplaysDeadLetterMessageToOriginalStream() throws Exception {
        backingStore.failSaves = true;
        MapRecord<String, String, String> originalRecord = enqueueAndRead(KinexisEvent.save(TestEntity.class, objectMapper.writeValueAsString(new TestEntity(8L, "Replay"))));
        String originalEventId = originalRecord.getValue().get(KinexisEvent.EVENT_ID_KEY);
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
        assertEquals(originalEventId, streamRecords.getLast().getValue().get(KinexisEvent.EVENT_ID_KEY));

        Optional<String> forcedReplayId = dlqService.replayWithNewEventId(TestEntity.class, dlqRecords.getFirst().getId().getValue());
        assertTrue(forcedReplayId.isPresent());
        List<MapRecord<String, Object, Object>> forcedStreamRecords = redisTemplate.opsForStream()
                .range(Misc.getStreamKey(TestEntity.class), Range.closed(forcedReplayId.get(), forcedReplayId.get()));
        assertNotNull(forcedStreamRecords);
        assertEquals(1, forcedStreamRecords.size());
        assertNotEquals(originalEventId, forcedStreamRecords.getFirst().getValue().get(KinexisEvent.EVENT_ID_KEY));
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
    void telemetryRecordsPendingRetryDlqAndReplayCounts() throws Exception {
        SimpleKinexisTelemetry telemetry = new SimpleKinexisTelemetry();
        backingStore.failSaves = true;
        enqueueAndRead(KinexisEvent.save(TestEntity.class, objectMapper.writeValueAsString(new TestEntity(14L, "TelemetryDlq"))));
        TestPendingMessageHandler handler = pendingHandler(processor, 1);
        inject(handler, "telemetry", telemetry);
        inject(handler, "dlqWriter", new KinexisDlqWriter(redisTemplate, new KinexisProcessingMetrics(), telemetry));

        handler.processPendingMessages();

        List<MapRecord<String, Object, Object>> dlqRecords = redisTemplate.opsForStream()
                .range(Misc.getDLQStreamKey(TestEntity.class), Range.unbounded());
        assertNotNull(dlqRecords);
        assertEquals(1, dlqRecords.size());
        KinexisDlqService dlqService = new KinexisDlqService(redisTemplate, telemetry);
        assertTrue(dlqService.replay(TestEntity.class, dlqRecords.getFirst().getId().getValue()).isPresent());

        KinexisTelemetrySnapshot snapshot = telemetry.snapshot();
        assertEquals(1, counter(snapshot, KinexisTelemetry.PENDING_RETRIES, "entity", "TestEntity"));
        assertEquals(1, counter(snapshot, KinexisTelemetry.DLQ_RECORDS, "reason", "Too many attempts"));
        assertEquals(1, counter(snapshot, KinexisTelemetry.DLQ_REPLAYS, "mode", KinexisDlqService.ReplayMode.REPLAY_ONLY.name()));
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

    private TestPendingMessageHandler pendingHandler(TestProcessor processor, int maxAttempts,
                                                     StreamPartitioner streamPartitioner) throws Exception {
        TestPendingMessageHandler handler = pendingHandler(processor, maxAttempts);
        inject(handler, "streamPartitioner", streamPartitioner);
        return handler;
    }

    private MapRecord<String, String, String> streamRecord(KinexisEvent event) {
        return StreamRecords.newRecord()
                .withId(RecordId.autoGenerate())
                .ofMap(event.toRecordMap())
                .withStreamKey(Misc.getStreamKey(TestEntity.class));
    }

    private void processUnchecked(MapRecord<String, String, String> record) {
        try {
            processor.process(record);
        } catch (ProcessMessageException e) {
            throw new RuntimeException(e);
        }
    }

    private Runnable awaitingTask(CountDownLatch release) {
        return () -> {
            try {
                release.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for release", e);
            }
        };
    }

    private boolean hasCause(Throwable throwable, Class<? extends Throwable> causeType) {
        Throwable current = throwable;
        while (current != null) {
            if (causeType.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
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

    private MapRecord<String, String, String> enqueueAndRead(KinexisEvent event, Class<?> consumerGroupType,
                                                             StreamPartitioner streamPartitioner) {
        String streamKey = streamPartitioner.streamKey(TestEntity.class, event);
        String group = Misc.getConsumerGroup(consumerGroupType);
        String consumer = Misc.getConsumerName(consumerGroupType);
        KinexisStreamLifecycle lifecycle = new KinexisStreamLifecycle(redisTemplate, null);
        streamPartitioner.streamKeys(TestEntity.class)
                .forEach(partitionStream -> lifecycle.ensureConsumerGroup(partitionStream, group));
        redisTemplate.opsForStream().add(StreamRecords.newRecord()
                .withId(RecordId.autoGenerate())
                .ofMap(event.toRecordMap())
                .withStreamKey(streamKey));
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

    private MapRecord<String, String, String> streamRecordById(String recordId) {
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream()
                .range(Misc.getStreamKey(TestEntity.class), Range.closed(recordId, recordId));
        assertNotNull(records);
        assertEquals(1, records.size());
        return convertRecord(records.getFirst());
    }

    private long pendingCount() {
        return pendingCount(TestEntity.class);
    }

    private long pendingCount(Class<?> consumerGroupType) {
        PendingMessagesSummary pending = redisTemplate.opsForStream()
                .pending(Misc.getStreamKey(TestEntity.class), Misc.getConsumerGroup(consumerGroupType));
        return pending == null ? 0 : pending.getTotalPendingMessages();
    }

    private long pendingCount(String streamKey, Class<?> consumerGroupType) {
        PendingMessagesSummary pending = redisTemplate.opsForStream()
                .pending(streamKey, Misc.getConsumerGroup(consumerGroupType));
        return pending == null ? 0 : pending.getTotalPendingMessages();
    }

    private long counter(KinexisTelemetrySnapshot snapshot, String name, String tagKey, String tagValue) {
        return snapshot.counters()
                .stream()
                .filter(sample -> sample.name().equals(name))
                .filter(sample -> tagValue.equals(sample.tags().get(tagKey)))
                .mapToLong(KinexisTelemetrySnapshot.CounterSample::count)
                .sum();
    }

    private long timerCount(KinexisTelemetrySnapshot snapshot, String name, String tagKey, String tagValue) {
        return snapshot.timers()
                .stream()
                .filter(sample -> sample.name().equals(name))
                .filter(sample -> tagValue.equals(sample.tags().get(tagKey)))
                .mapToLong(KinexisTelemetrySnapshot.TimerSample::count)
                .sum();
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

    private static final class RejectingStreamListener extends AbstractStreamListener<TestEntity> {

        private final RejectingProcessor processor;

        private RejectingStreamListener(RedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {
            this.processor = new RejectingProcessor(redisTemplate, objectMapper);
        }

        @Override
        public Processor<TestEntity> getProcessor() {
            return processor;
        }
    }

    private static final class RejectingProcessor implements Processor<TestEntity> {

        private final RedisTemplate<String, String> redisTemplate;
        private final ObjectMapper objectMapper;

        private RejectingProcessor(RedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {
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

        @Override
        public TestEntity convertToEntity(String content) throws com.fasterxml.jackson.core.JsonProcessingException {
            return objectMapper.readValue(content, TestEntity.class);
        }

        @Override
        public Class<TestEntity> getEntityClass() {
            return TestEntity.class;
        }

        @Override
        public void process(MapRecord<String, String, String> record) throws ProcessMessageException {
            throw new ProcessMessageException("backpressure", new KinexisBackpressureException("capacity"));
        }

        @Override
        public void acknowledge(MapRecord<String, String, String> record) {
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
        private KinexisEvent lastEvent;

        @Override
        public String append(Class<?> entityType, KinexisEvent event) {
            appendCount++;
            lastEvent = event;
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
        protected boolean failSaves;

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

    private static final class CountingStore extends InMemoryStore {

        private final AtomicInteger saveCount = new AtomicInteger();

        private CountingStore(String name) {
            super(name);
        }

        private CountingStore(String name, String... targets) {
            super(name, targets);
        }

        @Override
        public TestEntity save(TestEntity entity) {
            TestEntity saved = super.save(entity);
            saveCount.incrementAndGet();
            return saved;
        }

        private int saveCount() {
            return saveCount.get();
        }
    }

    private static final class ControlledStore extends InMemoryStore {

        private final CountDownLatch entered;
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger maxConcurrent = new AtomicInteger();
        private final AtomicInteger saveCount = new AtomicInteger();

        private ControlledStore(String name, int expectedEntries) {
            super(name);
            this.entered = new CountDownLatch(expectedEntries);
        }

        @Override
        public TestEntity save(TestEntity entity) {
            int current = active.incrementAndGet();
            maxConcurrent.accumulateAndGet(current, Math::max);
            entered.countDown();
            try {
                if (!release.await(2, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting for store release");
                }
                TestEntity saved = super.save(entity);
                saveCount.incrementAndGet();
                return saved;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted waiting for store release", e);
            } finally {
                active.decrementAndGet();
            }
        }

        private boolean awaitEntered() throws InterruptedException {
            return entered.await(2, TimeUnit.SECONDS);
        }

        private void release() {
            release.countDown();
        }

        private int maxConcurrent() {
            return maxConcurrent.get();
        }

        private int saveCount() {
            return saveCount.get();
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
