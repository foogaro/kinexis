[![Maven Central](https://img.shields.io/maven-central/v/io.github.foogaro/kinexis-core.svg)](https://search.maven.org/artifact/io.github.foogaro/kinexis-core)

# Kinexis

Kinexis is a modular Java library for cache-aside, write-behind, and refresh-ahead data flows built around explicit store adapters and Redis Streams.

The goal is to let application code keep using normal services and repositories while Kinexis handles cache reads, cache writes, durable stream events, asynchronous persistence, retry, dead-letter handling, replay, idempotency, ordering, backpressure, and lightweight telemetry.

Kinexis is intentionally store-agnostic. A backing store can be PostgreSQL, MySQL, SQL Server, MongoDB, Cassandra, Redis OM Spring, another Spring Data repository, an HTTP API, or a custom implementation. The runtime talks to stores through `EntityStore<T>` and `CacheStore<T>`, not through a database-specific API.

## Project Modules

Kinexis is split so applications can choose the smallest dependency surface that matches their runtime.

| Artifact | What it contains | Main dependency surface |
| --- | --- | --- |
| `kinexis-bom` | Maven BOM for aligning all Kinexis split module versions. | No runtime dependencies. |
| `kinexis-api` | `@CachingPatterns`, events, store interfaces, default registries, exceptions, telemetry contracts, and `KinexisProperties`. | No compile dependencies. |
| `kinexis-spring` | `KinexisService<T>`, annotation inspection, Spring bean discovery, Spring Data `CrudRepository` adapters, and optional Micrometer bridge. | Spring Context, Spring Data Commons, Jackson. |
| `kinexis-redis-streams` | Redis Streams publisher, generated-listener base classes, processors, pending retry, DLQ, replay, idempotency, partitioning, backpressure, validation, diagnostics, and Spring Redis configuration. | Spring Data Redis, Lettuce, Spring Boot autoconfigure. |
| `kinexis-redis-om` | Redis OM cache adapter and the annotation processor that generates Redis OM repositories plus entity stream components. | Redis OM Spring, JavaPoet, Kinexis Redis Streams runtime. |
| `kinexis-core` | Backward-compatible bundle for users that want one dependency. | Depends on all modules above. |

Existing imports remain stable. Even when using the split modules, public classes still live under packages such as `com.foogaro.kinexis.core.service`, `com.foogaro.kinexis.core.store`, and `com.foogaro.kinexis.core.model`.

## What Kinexis Does

| Capability | Behavior |
| --- | --- |
| Cache-aside | Reads from a cache store first, falls back to a primary store, then repopulates the cache. |
| Write-behind | Publishes save/delete events to Redis Streams and persists asynchronously to target backing stores. |
| Refresh-ahead | Uses Redis key expiration events to reload cache entries when TTL-based entries expire. |
| Store routing | Routes write-behind events to all stores or selected target groups such as `primary`, `mysql`, or `archive`. |
| Parallel fan-out | Invokes matching target stores concurrently through a bounded executor. |
| Partitioned streams | Routes events by `entityId` across partitioned Redis Streams while keeping each entity on a stable partition. |
| Per-entity ordering | Serializes records for the same entity ID inside the application instance. |
| Idempotency | Skips a store write when the same `{eventId, storeName}` has already succeeded. |
| Pending retry | Reprocesses pending Redis Stream records using configurable retry limits. |
| DLQ and replay | Moves exhausted failures to a dead-letter stream and can replay records to original or selected targets. |
| Backpressure | Bounds in-flight records and executor queue depth, with block, slow-down, or reject-to-DLQ behavior. |
| Telemetry | Exposes dependency-light counters/timers and optionally forwards to Micrometer when a `MeterRegistry` bean exists. |
| Validation and diagnostics | Validates store wiring at startup and exposes runtime metadata through service APIs. |

Kinexis does not add HTTP endpoints. Diagnostics and DLQ operations are plain services so applications can expose them through Actuator, internal controllers, jobs, CLI tools, tests, or logs.

## Choosing Dependencies

### Maven BOM

Use `kinexis-bom` to align split module versions from one place.

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.github.foogaro</groupId>
            <artifactId>kinexis-bom</artifactId>
            <version>2.1.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

After importing the BOM, omit versions on Kinexis dependencies:

```xml
<dependency>
    <groupId>io.github.foogaro</groupId>
    <artifactId>kinexis-spring</artifactId>
</dependency>
<dependency>
    <groupId>io.github.foogaro</groupId>
    <artifactId>kinexis-redis-streams</artifactId>
</dependency>
```

### Compatibility Bundle

Use `kinexis-core` when you want the same one-dependency setup as earlier releases.

```xml
<dependency>
    <groupId>io.github.foogaro</groupId>
    <artifactId>kinexis-core</artifactId>
    <version>2.1.0</version>
</dependency>
```

This brings `kinexis-api`, `kinexis-spring`, `kinexis-redis-streams`, and `kinexis-redis-om`.

### API Only

Use `kinexis-api` if you only need annotations, event metadata, store contracts, or telemetry contracts.

```xml
<dependency>
    <groupId>io.github.foogaro</groupId>
    <artifactId>kinexis-api</artifactId>
    <version>2.1.0</version>
</dependency>
```

This module has no compile dependencies. It is the right choice for shared model jars, custom store implementations, tests, or non-Spring code that only needs the contracts.

### Spring With Custom Stores

Use this set when your application wants `KinexisService<T>`, explicit stores, and Redis Streams, but does not need generated Redis OM repositories.

```xml
<dependency>
    <groupId>io.github.foogaro</groupId>
    <artifactId>kinexis-spring</artifactId>
    <version>2.1.0</version>
</dependency>
<dependency>
    <groupId>io.github.foogaro</groupId>
    <artifactId>kinexis-redis-streams</artifactId>
    <version>2.1.0</version>
</dependency>
```

### Spring With Redis OM Generation

Use this set when you want the annotation processor to generate Redis OM repositories, stream listeners, processors, pending handlers, and refresh-ahead listeners.

```xml
<dependency>
    <groupId>io.github.foogaro</groupId>
    <artifactId>kinexis-redis-om</artifactId>
    <version>2.1.0</version>
</dependency>
```

`kinexis-redis-om` brings the runtime modules it needs transitively. It also contributes the annotation processor through `META-INF/services/javax.annotation.processing.Processor`.

If your Maven build uses explicit annotation processor paths, add `kinexis-redis-om` there as well:

```xml
<annotationProcessorPaths>
    <path>
        <groupId>io.github.foogaro</groupId>
        <artifactId>kinexis-redis-om</artifactId>
        <version>2.1.0</version>
    </path>
</annotationProcessorPaths>
```

## Spring Boot Setup

Import Kinexis Redis Streams configuration and enable scheduling.

```java
import com.foogaro.kinexis.core.config.KinexisConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@Import(KinexisConfiguration.class)
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```

When using generated Redis OM repositories, also enable Redis OM repository scanning for the generated repository package or a parent package that includes it.

```java
import com.redis.om.spring.annotations.EnableRedisDocumentRepositories;

@EnableRedisDocumentRepositories(basePackages = "com.example")
```

Configure Redis and Kinexis.

```properties
spring.data.redis.host=localhost
spring.data.redis.port=6379

kinexis.stream.poll-timeout=1s
kinexis.stream.batch-size=100
kinexis.stream.partitions=1
kinexis.stream.listener.pending.max-attempts=3
kinexis.stream.listener.pending.max-retention=120000
kinexis.stream.listener.pending.batch-size=50
kinexis.stream.listener.pending.fixed-delay=300000

kinexis.processing.max-parallel-stores=4
kinexis.processing.idempotency.enabled=true
kinexis.processing.idempotency.retention=7d
kinexis.processing.ordering.per-entity-enabled=true
kinexis.processing.backpressure.max-in-flight-per-stream=0
kinexis.processing.backpressure.executor-queue-size=1024
kinexis.processing.backpressure.queue-full-behavior=BLOCK
kinexis.processing.backpressure.slow-down-delay=100ms

kinexis.validation.enabled=true
kinexis.validation.fail-fast=true
kinexis.stores.repository-discovery.enabled=false
```

`kinexis-redis-streams` includes Spring Boot configuration metadata for the `kinexis.*` namespace.

## Annotating Entities

`@CachingPatterns` is in `kinexis-api`. The annotation describes desired behavior. Runtime behavior is implemented by `KinexisService<T>` and the stream runtime.

```java
import com.foogaro.kinexis.core.annotation.CachingPatterns;
import com.foogaro.kinexis.core.model.CachingFormat;
import com.foogaro.kinexis.core.model.CachingPattern;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
@CachingPatterns(
        format = CachingFormat.JSON,
        patterns = {
                CachingPattern.CACHE_ASIDE,
                CachingPattern.WRITE_BEHIND,
                CachingPattern.REFRESH_AHEAD
        },
        ttl = 300
)
public class Employer {

    @Id
    private Long id;

    private String name;
    private String email;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}
```

Annotation options:

| Option | Meaning |
| --- | --- |
| `patterns` | Selects `CACHE_ASIDE`, `WRITE_BEHIND`, `REFRESH_AHEAD`, or `NONE`. |
| `format` | Selects generated Redis repository style: `JSON` maps to Redis OM document repositories, `HASH` maps to enhanced hash repositories. |
| `ttl` | TTL in seconds for cache writes. Values less than or equal to zero mean no expiration. |
| `enabled` | If `false`, `KinexisService` bypasses cache and stream behavior and delegates to the primary store. |

The Redis OM annotation processor expects an ID field annotated with `jakarta.persistence.Id` or `javax.persistence.Id`. Missing ID fields fail at compile time.

## Generated Code

Generated code is provided by `kinexis-redis-om`. For an entity named `Employer`, generated classes are placed under an entity-specific package:

```text
com.example.entity.employer.EmployerKinexisEntityRegistry
com.example.entity.employer.repository.EmployerRedisRepository
com.example.entity.employer.listener.EmployerStreamListener
com.example.entity.employer.processor.EmployerProcessor
com.example.entity.employer.handler.EmployerPendingMessageHandler
```

When `REFRESH_AHEAD` is enabled and `ttl > 0`, Kinexis also generates:

```text
com.example.entity.employer.listener.EmployerKeyExpirationListener
```

Generated processors are entity-based. They ask `EntityStoreRegistry` for stores at runtime, so one stream event can safely fan out to several configured backing stores.

## Service API

Application services extend `KinexisService<T>` from `kinexis-spring`.

```java
import com.foogaro.kinexis.core.service.KinexisService;
import org.springframework.stereotype.Service;

@Service
public class EmployerService extends KinexisService<Employer> {

    private final EmployerRepository repository;

    public EmployerService(EmployerRepository repository) {
        this.repository = repository;
    }

    public List<Employer> findAll() {
        return repository.findAll();
    }
}
```

Inherited methods:

```java
Optional<Employer> found = employerService.findById(42L);

Employer employer = new Employer();
employer.setId(42L);
employer.setName("Ada Lovelace");

employerService.save(employer);
employerService.update(employer);
employerService.delete(42L);
```

Runtime behavior depends on the annotation:

| Pattern | `findById` | `save` / `update` | `delete` |
| --- | --- | --- | --- |
| `CACHE_ASIDE` | Cache first, primary store fallback, then cache refill. | Writes to cache. | Deletes from cache. |
| `WRITE_BEHIND` | Cache behavior only if combined with cache-aside or refresh-ahead. | Publishes a Redis Stream save event. | Publishes a Redis Stream delete event. |
| `REFRESH_AHEAD` | Cache first, primary store fallback, cache refill with TTL. | Writes to cache with TTL. | Deletes from cache. |
| `enabled = false` | Reads from primary store. | Writes to primary store. | Deletes from primary store. |

Custom query methods remain your responsibility:

```java
public Employer findByEmail(String email) {
    return repository.findByEmail(email);
}
```

## Store Registration

Kinexis resolves stores through `EntityStoreRegistry`. Prefer explicit store beans. Repository-name discovery is deprecated and disabled by default.

### Primary Store

Use `CrudRepositoryEntityStore` from `kinexis-spring` for a Spring Data repository that should receive durable writes.

```java
import com.foogaro.kinexis.core.service.BeanFinder;
import com.foogaro.kinexis.core.store.CrudRepositoryEntityStore;
import com.foogaro.kinexis.core.store.EntityStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class EmployerStores {

    @Bean
    EntityStore<Employer> mysqlEmployerStore(
            EmployerMysqlRepository repository,
            BeanFinder beanFinder
    ) {
        return CrudRepositoryEntityStore
                .builder(Employer.class, repository, beanFinder)
                .name("mysqlEmployerStore")
                .targets("mysql", "primary")
                .build();
    }
}
```

### Redis OM Cache Store

Use `RedisOmCacheStore` from `kinexis-redis-om` for Redis OM Spring repositories. It honors TTL by expiring the resolved Redis entity key after save.

```java
import com.foogaro.kinexis.core.service.BeanFinder;
import com.foogaro.kinexis.core.store.CacheStore;
import com.foogaro.kinexis.core.store.RedisOmCacheStore;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.RedisTemplate;

@Bean
CacheStore<Employer> redisEmployerCache(
        EmployerRedisRepository repository,
        BeanFinder beanFinder,
        RedisTemplate<String, String> redisTemplate
) {
    return RedisOmCacheStore
            .builder(Employer.class, repository, beanFinder)
            .name("redisEmployerCache")
            .targets("redis", "cache")
            .redisTemplate(redisTemplate)
            .build();
}
```

### Generic Spring Data Cache Store

Use `CrudRepositoryCacheStore` from `kinexis-spring` when the cache repository is Spring Data compatible but not Redis OM specific.

```java
import com.foogaro.kinexis.core.store.CacheStore;
import com.foogaro.kinexis.core.store.CrudRepositoryCacheStore;

@Bean
CacheStore<Employer> genericEmployerCache(
        EmployerCacheRepository repository,
        BeanFinder beanFinder
) {
    return CrudRepositoryCacheStore
            .builder(Employer.class, repository, beanFinder)
            .name("genericEmployerCache")
            .targets("cache")
            .build();
}
```

`CrudRepositoryCacheStore` ignores TTL by default. Custom cache stores can honor TTL by overriding `save(T entity, Duration ttl)`.

```java
import java.time.Duration;

public class ExpiringCacheStore implements CacheStore<Employer> {

    @Override
    public Employer save(Employer entity, Duration ttl) {
        // Persist entity and apply ttl in your cache backend.
        return entity;
    }

    // Implement the remaining EntityStore methods.
}
```

### Custom Store

Implement `EntityStore<T>` for any backend.

```java
import com.foogaro.kinexis.core.store.EntityStore;
import java.util.Optional;
import java.util.Set;

public class HttpEntityStore implements EntityStore<Employer> {

    @Override
    public String name() {
        return "httpEmployerStore";
    }

    @Override
    public Class<Employer> entityType() {
        return Employer.class;
    }

    @Override
    public Set<String> targets() {
        return Set.of("http", "archive");
    }

    @Override
    public Optional<Employer> findById(Object id) {
        return Optional.empty();
    }

    @Override
    public Employer save(Employer entity) {
        return entity;
    }

    @Override
    public void deleteById(Object id) {
    }
}
```

Register it as a Spring bean:

```java
@Bean
EntityStore<Employer> httpEmployerStore() {
    return new HttpEntityStore();
}
```

## Targeted Write-Behind

Write-behind events can target all stores or selected store groups.

```java
employerService.save(employer);              // all target stores
employerService.save(employer, "primary");   // stores tagged primary
employerService.save(employer, "mysql");     // stores tagged mysql
employerService.delete(42L, "archive");      // stores tagged archive
```

`KinexisService` validates selected targets before publishing. If no configured store matches the requested target, it throws `IllegalArgumentException` and does not append to Redis Streams.

Each stream event carries:

| Field | Meaning |
| --- | --- |
| `eventId` | Stable logical event ID used for idempotency. |
| `entityType` | Fully qualified entity class name. |
| `entityId` | Entity ID when Kinexis can resolve it. |
| `operation` | `SAVE` or `DELETE`. |
| `content` | JSON entity payload for saves, ID value for deletes. |
| `targets` | Optional comma-separated target aliases. |
| `schemaVersion` | Event schema version. |
| `timestamp` | Event creation timestamp. |

## Redis Streams Runtime

The Redis Streams runtime is in `kinexis-redis-streams`.

Write-behind processing is at-least-once at the Redis Streams level. Kinexis adds store-level idempotency. If one target store succeeds and another fails, retry skips the successful store and only attempts unfinished stores.

When `kinexis.stream.partitions > 1`, records route by `entityId` to:

```text
wb:stream:entity:<entity>:partition:<n>
```

With one partition, the stream key is:

```text
wb:stream:entity:<entity>
```

Generated listeners subscribe to all partitions for the entity consumer group. Processors also apply local per-entity ordering so records for the same `entityId` are serialized inside one application instance.

## Backpressure

Kinexis bounds asynchronous store fan-out so write-behind does not become unbounded work.

```properties
kinexis.processing.max-parallel-stores=4
kinexis.processing.backpressure.executor-queue-size=1024
kinexis.processing.backpressure.max-in-flight-per-stream=0
kinexis.processing.backpressure.queue-full-behavior=BLOCK
kinexis.processing.backpressure.slow-down-delay=100ms
```

Queue-full behavior:

| Value | Behavior |
| --- | --- |
| `BLOCK` | Waits for executor queue capacity. |
| `SLOW_DOWN` | Sleeps between capacity checks using `slow-down-delay`. |
| `REJECT_TO_DLQ` | Rejects the record so it can be moved to the DLQ path. |

`KinexisProcessingMetrics` exposes queue depth, active worker count, rejections, slowdowns, pending retries, and DLQ counts without requiring a metrics dependency.

## Telemetry

`KinexisTelemetry` is dependency-light and lives in `kinexis-api`. The default runtime uses `SimpleKinexisTelemetry`. If Micrometer is already present and a `MeterRegistry` bean exists, `kinexis-spring` also forwards metrics to Micrometer through reflection.

Core metrics:

| Metric | Type | Tags |
| --- | --- | --- |
| `kinexis.stream.events.published` | Counter | `entity`, `operation`, `stream` |
| `kinexis.stream.events.processed` | Counter | `entity`, `operation`, `stream` |
| `kinexis.store.write.latency` | Timer | `entity`, `store`, `operation` |
| `kinexis.store.failures` | Counter | `entity`, `store`, `operation`, `exception` |
| `kinexis.pending.retries` | Counter | `entity`, `stream`, `group` |
| `kinexis.dlq.records` | Counter | `entity`, `stream`, `reason` |
| `kinexis.dlq.replays` | Counter | `entity`, `stream`, `mode` |
| `kinexis.cache.hits` | Counter | `entity` |
| `kinexis.cache.misses` | Counter | `entity` |

Read the in-memory snapshot:

```java
import com.foogaro.kinexis.core.telemetry.KinexisTelemetry;
import com.foogaro.kinexis.core.telemetry.KinexisTelemetrySnapshot;

KinexisTelemetrySnapshot snapshot = telemetry.snapshot();
```

## Validation And Diagnostics

Startup validation is enabled by default.

```properties
kinexis.validation.enabled=true
kinexis.validation.fail-fast=true
```

Validation checks:

* `kinexis.processing.max-parallel-stores >= 1`
* `kinexis.stream.partitions >= 1`
* cache patterns have a configured `CacheStore`
* cache-aside and refresh-ahead have a configured primary `EntityStore`
* write-behind has at least one target `EntityStore`
* disabled entities have a primary `EntityStore`
* store names are unique per entity
* target aliases are not ambiguous across stores for the same entity

Validation warnings include refresh-ahead without a positive TTL and deprecated repository discovery.

Inject `KinexisDiagnosticsService` to inspect runtime wiring.

```java
import com.foogaro.kinexis.core.service.KinexisDiagnosticsService;
import org.springframework.stereotype.Service;

@Service
public class KinexisAdminService {

    private final KinexisDiagnosticsService diagnosticsService;

    public KinexisAdminService(KinexisDiagnosticsService diagnosticsService) {
        this.diagnosticsService = diagnosticsService;
    }

    public List<KinexisDiagnosticsService.EntityDiagnostics> stores() {
        return diagnosticsService.stores();
    }
}
```

Diagnostics include entity class, enabled flag, patterns, TTL, cache store, primary store, target stores, and all known stores for duplicate or ambiguity checks.

## Dead-Letter Queue And Replay

Pending records are retried using Redis Stream pending-entry-list state. When a message exceeds the configured attempt limit, Kinexis writes it to a dead-letter stream.

DLQ records include structured metadata:

* `reason`
* `error`
* `attempts`
* `failedStore`
* `exceptionClass`
* `failureTimestamp`
* original stream key
* original stream ID
* original consumer group
* original consumer

Replay a dead-letter record:

```java
import com.foogaro.kinexis.core.service.KinexisDlqService;

Optional<String> replayedId = kinexisDlqService.replay(
        Employer.class,
        dlqRecordId
);
```

Replay to selected targets:

```java
kinexisDlqService.replay(Employer.class, dlqRecordId, "mysql", "primary");
```

Replay and delete the DLQ record after successful append:

```java
kinexisDlqService.replay(
        Employer.class,
        dlqRecordId,
        KinexisDlqService.ReplayMode.REPLAY_AND_DELETE,
        "mysql"
);
```

## Repository Discovery Migration

Explicit `EntityStore<T>` and `CacheStore<T>` beans are the preferred API. Legacy repository-name discovery still exists as a migration bridge and is disabled by default.

```properties
kinexis.stores.repository-discovery.enabled=true
```

When enabled, Kinexis looks for Spring Data repositories by naming convention:

| Repository name | Role |
| --- | --- |
| `<Entity>RedisRepository` | Cache repository. |
| `<Entity>Repository` | Primary repository. |
| other Spring Data repositories for the entity | Write-behind target stores. |

This fallback uses generic Spring Data `CrudRepository` adapters. Register `RedisOmCacheStore` explicitly when you need true Redis TTL support.

## Configuration Reference

| Property | Default | Description |
| --- | --- | --- |
| `kinexis.stream.poll-timeout` | `1s` | Redis Stream poll timeout. |
| `kinexis.stream.batch-size` | `100` | Records read per stream poll. |
| `kinexis.stream.partitions` | `1` | Number of Redis Stream partitions per entity. Values greater than one route records by `entityId`. |
| `kinexis.stream.listener.pending.max-attempts` | `3` | Attempts before DLQ. |
| `kinexis.stream.listener.pending.max-retention` | `120000` | Pending retention threshold in milliseconds. |
| `kinexis.stream.listener.pending.batch-size` | `50` | Pending records inspected per scan. |
| `kinexis.stream.listener.pending.fixed-delay` | `300000` | Scheduler delay for pending scans in milliseconds. |
| `kinexis.processing.max-parallel-stores` | available processors, minimum 2 | Maximum concurrent target-store calls per stream record. |
| `kinexis.processing.idempotency.enabled` | `true` | Skips target-store operations already completed for the same event/store pair. |
| `kinexis.processing.idempotency.retention` | `7d` | Retention for Redis idempotency markers. |
| `kinexis.processing.ordering.per-entity-enabled` | `true` | Enables local per-entity processing locks. |
| `kinexis.processing.backpressure.max-in-flight-per-stream` | `0` | Maximum active records per Redis Stream key. Use `0` for no per-stream cap. |
| `kinexis.processing.backpressure.executor-queue-size` | `1024` | Bounded queue size for asynchronous target-store work. |
| `kinexis.processing.backpressure.queue-full-behavior` | `BLOCK` | Overload behavior: `BLOCK`, `SLOW_DOWN`, or `REJECT_TO_DLQ`. |
| `kinexis.processing.backpressure.slow-down-delay` | `100ms` | Delay between capacity checks when behavior is `SLOW_DOWN`. |
| `kinexis.validation.enabled` | `true` | Enables startup validation. |
| `kinexis.validation.fail-fast` | `true` | Fails startup when validation errors exist. |
| `kinexis.stores.repository-discovery.enabled` | `false` | Enables deprecated repository-name discovery. |

## Testing The Project

Run the full test suite:

```bash
./mvnw clean test
```

Run only the compatibility bundle and its integration tests:

```bash
./mvnw -pl kinexis-core -am test
```

Run a demo module:

```bash
./mvnw -pl demo/kinexis-demo-psql -am test
```

The test suite uses Testcontainers and covers Redis Streams append, save, delete, failed processing, pending retry, DLQ, replay, TTL, explicit stores, repository discovery opt-in, diagnostics, validation, target routing, parallel fan-out, backpressure, partitioning, idempotency, telemetry, and generated-code compatibility.

## Demo Projects

The repository includes demo applications for common backing stores:

| Module | Backing store focus |
| --- | --- |
| `demo/kinexis-demo` | Combined multi-datasource demo. |
| `demo/kinexis-demo-psql` | PostgreSQL. |
| `demo/kinexis-demo-mysql` | MySQL. |
| `demo/kinexis-demo-mongodb` | MongoDB. |
| `demo/kinexis-demo-sql-server` | SQL Server. |
| `demo/kinexis-demo-cassandra` | Cassandra. |

## Design Notes

Kinexis keeps the API small:

* `@CachingPatterns` describes desired behavior.
* `KinexisService<T>` is the Spring service base class used by application code.
* `EntityStore<T>` and `CacheStore<T>` keep store access explicit and platform-agnostic.
* `EntityStoreRegistry` resolves cache, primary, and target stores.
* `EventPublisher` abstracts event publication.
* `KinexisTelemetry` abstracts operational metrics.
* `KinexisDiagnosticsService` exposes runtime metadata.
* `KinexisDlqService` handles operational replay.

Redis is used for cache and streams, but durable backing stores are intentionally not Redis-specific.

## Disclaimer

Kinexis is a personal project and is not affiliated with or endorsed by Redis Inc.
