[![Maven Central](https://img.shields.io/maven-central/v/io.github.foogaro/kinexis-core.svg)](https://search.maven.org/artifact/io.github.foogaro/kinexis-core)

# Kinexis

Kinexis is a lightweight Spring Boot library for implementing cache-aside, write-behind, and refresh-ahead data flows with Redis Streams.

The core idea is simple: application code keeps using ordinary services and repositories, while Kinexis coordinates cache reads, cache writes, durable stream events, asynchronous persistence, retry, and dead-letter recovery.

Kinexis is designed around explicit store adapters, not around a specific database technology. A store can be backed by JPA, MongoDB, Redis OM Spring, another Spring Data repository, or a custom implementation. The runtime works through `EntityStore<T>` and `CacheStore<T>` so the processing model stays platform-agnostic.

## What It Does

Kinexis provides:

| Capability | What Kinexis does |
| --- | --- |
| Cache-aside | Reads from cache first, falls back to a primary store, then repopulates the cache. |
| Write-behind | Publishes save/delete events to Redis Streams and persists to one or more backing stores asynchronously. |
| Refresh-ahead | Uses Redis key expiration events to refresh cached data when TTL-based cache entries expire. |
| Explicit store routing | Lets you register named stores and target groups such as `mysql`, `mongo`, `primary`, or `archive`. |
| Parallel fan-out | Writes one stream event to multiple target stores concurrently through a bounded executor. |
| Retry and DLQ | Retries pending stream records and moves exhausted failures to a dead-letter stream with structured metadata. |
| DLQ replay | Replays dead-letter records to the original stream, optionally overriding target stores. |
| Startup validation | Fails fast for missing stores, invalid parallelism, duplicate store names, and ambiguous target aliases. |
| Diagnostics | Exposes resolved entity/store metadata through `KinexisDiagnosticsService`. |

Kinexis does not add an HTTP diagnostics endpoint. The library exposes a service API so applications can decide whether to wire that into Actuator, an internal controller, logs, tests, or another operational surface.

## Runtime Model

For an annotated entity, Kinexis generates entity-specific infrastructure at compile time:

* a Redis OM repository for the cache representation
* a Redis Stream listener
* a stream processor
* a pending-message handler
* a lightweight `KinexisEntityRegistry` component
* a key-expiration listener when refresh-ahead is enabled with a positive TTL

At runtime, the flow is:

1. Your application calls a method inherited from `KinexisService<T>`.
2. Reads consult `EntityStoreRegistry` for a `CacheStore<T>` and primary `EntityStore<T>`.
3. Write-behind saves and deletes append versioned events to Redis Streams.
4. Generated stream listeners read those events by entity-specific consumer group.
5. Processors resolve target stores from `EntityStoreRegistry`.
6. Target stores are invoked concurrently up to `kinexis.processing.max-parallel-stores`.
7. Failed processing stays pending, is retried, and eventually moves to a dead-letter stream.

## Installation

Add `kinexis-core` to your Spring Boot application.

```xml
<dependency>
    <groupId>io.github.foogaro</groupId>
    <artifactId>kinexis-core</artifactId>
    <version>1.0.1</version>
</dependency>
```

Import Kinexis configuration in your Spring Boot application:

```java
import com.foogaro.kinexis.core.config.KinexisConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableRedisRepositories(basePackages = "com.example")
@Import(KinexisConfiguration.class)
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```

Configure Redis and Kinexis:

```properties
spring.data.redis.host=localhost
spring.data.redis.port=6379

kinexis.stream.poll-timeout=1s
kinexis.stream.batch-size=100
kinexis.stream.listener.pending.max-attempts=3
kinexis.stream.listener.pending.max-retention=120000
kinexis.stream.listener.pending.batch-size=50
kinexis.stream.listener.pending.fixed-delay=300000

kinexis.processing.max-parallel-stores=4
kinexis.validation.enabled=true
kinexis.validation.fail-fast=true
kinexis.stores.repository-discovery.enabled=false
```

The jar includes Spring Boot configuration metadata for the `kinexis.*` namespace, so IDEs can autocomplete these properties without extra runtime dependencies.

## Annotating An Entity

Use `@CachingPatterns` on the entity class.

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

`@CachingPatterns` options:

| Option | Meaning |
| --- | --- |
| `patterns` | Selects `CACHE_ASIDE`, `WRITE_BEHIND`, `REFRESH_AHEAD`, or `NONE`. |
| `format` | Selects Redis cache format: `JSON` or `HASH`. |
| `ttl` | TTL in seconds for cache writes. A value less than or equal to zero means no expiration. |
| `enabled` | If `false`, `KinexisService` bypasses cache and stream behavior and delegates to the primary store. |

The annotation processor expects an ID field annotated with `jakarta.persistence.Id` or `javax.persistence.Id`.

## Service API

Application services extend `KinexisService<T>`.

```java
import com.foogaro.kinexis.core.service.KinexisService;
import org.springframework.stereotype.Service;

@Service
public class EmployerService extends KinexisService<Employer> {

    private final EmployerMysqlRepository repository;

    public EmployerService(EmployerMysqlRepository repository) {
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

For write-behind entities, `save`, `update`, and `delete` publish stream events instead of directly calling the database. For cache-aside entities, `findById` reads from cache first and then falls back to the primary store.

Custom repository queries remain your responsibility:

```java
public Employer findByEmail(String email) {
    return repository.findByEmail(email);
}
```

## Store Registration

Kinexis resolves stores through `EntityStoreRegistry`. The standard path is to define explicit `EntityStore<T>` and `CacheStore<T>` beans.

Repository-name discovery is deprecated and disabled by default. Use it only as a migration bridge:

```properties
kinexis.stores.repository-discovery.enabled=true
```

### Primary Store With CrudRepositoryEntityStore

Use `CrudRepositoryEntityStore` for a Spring Data repository that should receive durable writes.

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

### Cache Store With RedisOmCacheStore

Use `RedisOmCacheStore` for Redis OM Spring repositories. It honors TTL by expiring the resolved Redis entity key after save.

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

### Cache Store With CrudRepositoryCacheStore

Use `CrudRepositoryCacheStore` when the cache repository is Spring Data compatible but not Redis OM specific.

```java
@Bean
CacheStore<Employer> genericCacheStore(
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

`CrudRepositoryCacheStore` ignores TTL by default. Any `CacheStore<T>` can support TTL by overriding:

```java
default T save(T entity, Duration ttl) {
    return save(entity);
}
```

### Custom Stores

Implement `EntityStore<T>` for any platform.

```java
import com.foogaro.kinexis.core.store.EntityStore;

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

Implement `CacheStore<T>` when the store should be used as the cache.

```java
public class LocalCacheStore implements CacheStore<Employer> {
    // same EntityStore methods, plus optional TTL support
}
```

## Targeted Writes

Write-behind events can target all backing stores or a selected group.

```java
employerService.save(employer);              // all target stores
employerService.save(employer, "primary");   // stores tagged primary
employerService.save(employer, "mysql");     // stores tagged mysql
employerService.delete(42L, "archive");      // stores tagged archive
```

`KinexisService` validates selected targets before publishing a stream event. If no configured store matches the requested target, it throws an `IllegalArgumentException` and does not append to Redis Streams.

If an invalid event is appended directly to Redis Streams, the processor fails the message and leaves it pending for retry/DLQ handling.

## Parallel Fan-Out

One stream event can write to multiple stores. Kinexis invokes matching target stores concurrently through a bounded executor.

```properties
kinexis.processing.max-parallel-stores=4
```

If one or more stores fail, the processor raises `ProcessMessageException` with the failed store names. The pending-message handler records the failure metadata in the dead-letter stream when retry attempts are exhausted.

## Validation

Startup validation is enabled by default.

```properties
kinexis.validation.enabled=true
kinexis.validation.fail-fast=true
```

Validation checks:

* `kinexis.processing.max-parallel-stores >= 1`
* cache patterns have a configured `CacheStore`
* cache-aside and refresh-ahead have a configured primary `EntityStore`
* write-behind has at least one target `EntityStore`
* disabled entities have a primary `EntityStore` for direct delegation
* store names are unique per entity
* target aliases are not ambiguous across stores for the same entity

Validation warnings:

* `REFRESH_AHEAD` is configured without a positive TTL
* deprecated repository discovery is enabled

Entity discovery for validation comes from:

* generated `KinexisEntityRegistry` components
* explicit store beans
* generated processors
* `KinexisService` beans

## Diagnostics

Inject `KinexisDiagnosticsService` to inspect the runtime configuration.

```java
import com.foogaro.kinexis.core.service.KinexisDiagnosticsService;

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

Each `EntityDiagnostics` contains:

* entity class and name
* whether the entity is annotated
* whether Kinexis is enabled for the entity
* configured caching patterns
* TTL
* resolved cache store
* resolved primary store
* target stores
* all known stores for duplicate/ambiguity checks

## Dead-Letter Queue And Replay

Pending records are retried using Redis Stream pending-entry-list state. When a message exceeds the configured attempt limit, Kinexis writes it to a dead-letter stream with structured metadata.

DLQ fields include:

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

## Configuration Reference

| Property | Default | Description |
| --- | --- | --- |
| `kinexis.stream.poll-timeout` | `1s` | Redis Stream poll timeout. |
| `kinexis.stream.batch-size` | `100` | Records read per stream poll. |
| `kinexis.stream.listener.pending.max-attempts` | `3` | Attempts before DLQ. |
| `kinexis.stream.listener.pending.max-retention` | `120000` | Pending retention threshold in milliseconds. |
| `kinexis.stream.listener.pending.batch-size` | `50` | Pending records inspected per scan. |
| `kinexis.stream.listener.pending.fixed-delay` | `300000` | Scheduler delay for pending scans in milliseconds. |
| `kinexis.processing.max-parallel-stores` | available processors, minimum 2 | Maximum concurrent target-store calls per stream record. |
| `kinexis.validation.enabled` | `true` | Enables startup validation. |
| `kinexis.validation.fail-fast` | `true` | Fails startup when validation errors exist. |
| `kinexis.stores.repository-discovery.enabled` | `false` | Enables deprecated repository-name discovery. |

## Generated Components

For an entity named `Employer`, generated code is placed under an entity-specific package such as:

```text
com.example.entity.employer.repository.EmployerRedisRepository
com.example.entity.employer.listener.EmployerStreamListener
com.example.entity.employer.processor.EmployerProcessor
com.example.entity.employer.handler.EmployerPendingMessageHandler
com.example.entity.employer.EmployerKinexisEntityRegistry
```

Refresh-ahead with positive TTL also generates:

```text
com.example.entity.employer.listener.EmployerKeyExpirationListener
```

The generated processor is entity-based, not repository-based. It asks `EntityStoreRegistry` for stores at runtime, which is what allows one event to fan out to several backing platforms.

## Testing

Run the full test suite:

```bash
./mvnw clean test
```

The integration tests use Testcontainers with Redis and cover stream append, save, delete, failed processing, pending retry, DLQ, replay, TTL, explicit stores, repository discovery opt-in, diagnostics, validation, target routing, and parallel fan-out.

## Design Notes

Kinexis intentionally keeps the public runtime small:

* `@CachingPatterns` describes desired behavior.
* `KinexisService<T>` is the service base class used by application code.
* `EntityStore<T>` and `CacheStore<T>` make stores explicit and platform-agnostic.
* `EntityStoreRegistry` resolves stores for services and processors.
* `KinexisDiagnosticsService` exposes runtime metadata.
* `KinexisDlqService` handles operational replay.

The library uses Redis for cache and streams, but backing stores are intentionally not Redis-specific.

## Disclaimer

Kinexis is a personal project and is not affiliated with or endorsed by Redis Inc.
