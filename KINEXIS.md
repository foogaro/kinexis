# Using Kinexis In An Application

This guide is for application developers who want to use Kinexis with Redis Streams, a Redis-backed cache, and multiple backing databases such as MySQL and PostgreSQL.

Kinexis lets your service code call normal methods such as `save`, `delete`, and `findById`, while the library handles cache-aside reads, write-behind stream events, asynchronous fan-out to target stores, retry, per-store DLQ records, replay, idempotency, ordering, backpressure, and telemetry.

## What You Implement

In a typical Spring Boot application, you implement:

1. An entity annotated with `@CachingPatterns`.
2. Spring Data repositories for your backing stores, for example MySQL and PostgreSQL.
3. A Redis cache repository or another `CacheStore`.
4. Explicit `EntityStore<T>` beans for each backing database.
5. One `CacheStore<T>` bean.
6. A service that extends `KinexisService<T>`.

Your application code depends on `KinexisService<T>` and store interfaces. It does not call Redis Streams or database-specific write-behind processors directly.

## Dependencies

Prefer the BOM when using split modules:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.github.foogaro</groupId>
            <artifactId>kinexis-bom</artifactId>
            <version>2.6.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```

For a Spring Boot application with Redis Streams and explicit stores:

```xml
<dependencies>
    <dependency>
        <groupId>io.github.foogaro</groupId>
        <artifactId>kinexis-spring</artifactId>
    </dependency>
    <dependency>
        <groupId>io.github.foogaro</groupId>
        <artifactId>kinexis-redis-streams</artifactId>
    </dependency>
    <dependency>
        <groupId>io.github.foogaro</groupId>
        <artifactId>kinexis-redis-om</artifactId>
    </dependency>
</dependencies>
```

You still add your own Spring Boot database dependencies, for example Spring Data JPA plus MySQL and PostgreSQL drivers.

## Spring Boot Setup

Import Kinexis configuration and enable scheduling:

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

If you use Redis OM repositories, enable Redis document repository scanning for your package:

```java
import com.redis.om.spring.annotations.EnableRedisDocumentRepositories;

@EnableRedisDocumentRepositories(basePackages = "com.example")
```

## Configuration

Configure Redis and Kinexis:

```properties
spring.data.redis.host=localhost
spring.data.redis.port=6379

kinexis.stream.poll-timeout=1s
kinexis.stream.batch-size=100
kinexis.stream.partitions=4

kinexis.stream.listener.pending.max-attempts=3
kinexis.stream.listener.pending.max-retention=120000
kinexis.stream.listener.pending.batch-size=50
kinexis.stream.listener.pending.fixed-delay=300000

kinexis.processing.max-parallel-stores=4
kinexis.processing.idempotency.enabled=true
kinexis.processing.idempotency.retention=7d
kinexis.processing.ordering.per-entity-enabled=true
kinexis.processing.backpressure.executor-queue-size=1024
kinexis.processing.backpressure.queue-full-behavior=BLOCK

kinexis.store-health.enabled=true
kinexis.store-health.failure-threshold=10
kinexis.store-health.open-duration=60s
kinexis.store-health.probe-success-threshold=3
kinexis.store-health.failure-window=5m

kinexis.validation.enabled=true
kinexis.validation.fail-fast=true
kinexis.stores.repository-discovery.enabled=false
```

With multiple SQL databases, configure each datasource and repository package in your application as usual. Kinexis only needs the resulting repository beans or custom store beans.

## Entity

Annotate the entity with the caching patterns you want Kinexis to honor:

```java
package com.example.employer;

import com.foogaro.kinexis.core.annotation.CachingPatterns;
import com.foogaro.kinexis.core.model.CachingFormat;
import com.foogaro.kinexis.core.model.CachingPattern;
import com.redis.om.spring.annotations.Document;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
@Document("employers")
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
    @org.springframework.data.annotation.Id
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

Useful annotation behavior:

| Option | Effect |
| --- | --- |
| `CACHE_ASIDE` | `findById` reads cache first, then primary store, then refills cache. |
| `WRITE_BEHIND` | `save`, `update`, and `delete` publish Redis Stream events for asynchronous persistence. |
| `REFRESH_AHEAD` | Cache writes use TTL, and Redis expiration can trigger refresh behavior. |
| `ttl` | Cache TTL in seconds. `RedisOmCacheStore` honors it. Generic cache stores may ignore it. |
| `enabled = false` | Kinexis bypasses cache and stream behavior and delegates to the primary store. |

## Repositories

Create one repository per backing store.

```java
package com.example.employer.repository.mysql;

import com.example.employer.Employer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EmployerMysqlRepository extends JpaRepository<Employer, Long> {
}
```

```java
package com.example.employer.repository.postgres;

import com.example.employer.Employer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EmployerPostgresRepository extends JpaRepository<Employer, Long> {
}
```

For Redis OM cache:

```java
package com.example.employer.repository.redis;

import com.example.employer.Employer;
import com.redis.om.spring.repository.RedisDocumentRepository;

public interface EmployerRedisRepository extends RedisDocumentRepository<Employer, Long> {
}
```

## Store Beans

Register explicit store beans. Kinexis prefers these over repository-name discovery.

```java
package com.example.employer;

import com.example.employer.repository.mysql.EmployerMysqlRepository;
import com.example.employer.repository.postgres.EmployerPostgresRepository;
import com.example.employer.repository.redis.EmployerRedisRepository;
import com.foogaro.kinexis.core.service.BeanFinder;
import com.foogaro.kinexis.core.store.CacheStore;
import com.foogaro.kinexis.core.store.CrudRepositoryEntityStore;
import com.foogaro.kinexis.core.store.EntityStore;
import com.foogaro.kinexis.core.store.RedisOmCacheStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

@Configuration
class EmployerStoreConfiguration {

    @Bean
    EntityStore<Employer> employerMysqlStore(
            EmployerMysqlRepository repository,
            BeanFinder beanFinder
    ) {
        return CrudRepositoryEntityStore
                .builder(Employer.class, repository, beanFinder)
                .name("mysql")
                .targets("mysql", "primary")
                .build();
    }

    @Bean
    EntityStore<Employer> employerPostgresStore(
            EmployerPostgresRepository repository,
            BeanFinder beanFinder
    ) {
        return CrudRepositoryEntityStore
                .builder(Employer.class, repository, beanFinder)
                .name("postgresql")
                .targets("postgresql", "primary")
                .build();
    }

    @Bean
    CacheStore<Employer> employerRedisCacheStore(
            EmployerRedisRepository repository,
            BeanFinder beanFinder,
            RedisTemplate<String, String> redisTemplate
    ) {
        return RedisOmCacheStore
                .builder(Employer.class, repository, beanFinder)
                .name("redis")
                .targets("redis", "cache")
                .redisTemplate(redisTemplate)
                .build();
    }
}
```

Store names identify a concrete store. Targets are aliases used for routing. In the example:

| Store | Name | Targets |
| --- | --- | --- |
| MySQL | `mysql` | `mysql`, `primary` |
| PostgreSQL | `postgresql` | `postgresql`, `primary` |
| Redis cache | `redis` | `redis`, `cache` |

Calling `save(entity)` writes behind to all configured target entity stores. Calling `save(entity, "mysql")` only writes to stores tagged `mysql`. Calling `save(entity, "primary")` writes to both MySQL and PostgreSQL in this example.

## Service

Application services extend `KinexisService<T>`:

```java
package com.example.employer;

import com.example.employer.repository.postgres.EmployerPostgresRepository;
import com.foogaro.kinexis.core.service.KinexisService;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EmployerService extends KinexisService<Employer> {

    private final EmployerPostgresRepository readRepository;

    public EmployerService(EmployerPostgresRepository readRepository) {
        this.readRepository = readRepository;
    }

    public List<Employer> findAll() {
        return readRepository.findAll();
    }
}
```

`KinexisService` provides:

```java
employerService.findById(1L);
employerService.save(employer);
employerService.update(employer);
employerService.delete(1L);
```

Targeted write-behind:

```java
employerService.save(employer);                 // MySQL and PostgreSQL
employerService.save(employer, "mysql");        // MySQL only
employerService.save(employer, "postgresql");   // PostgreSQL only
employerService.save(employer, "primary");      // MySQL and PostgreSQL

employerService.delete(1L, "mysql");
```

Custom queries such as `findAll`, `findByEmail`, or search endpoints remain application code. Kinexis controls the common CRUD methods inherited from `KinexisService`.

## Runtime Behavior

For `findById(id)`:

1. Kinexis reads the `CacheStore`.
2. On a cache hit, it returns the cached entity.
3. On a cache miss, it reads the primary `EntityStore`.
4. If the entity exists, it writes it back to cache using the configured TTL.

For `save(entity)` or `delete(id)` with write-behind:

1. Kinexis validates target stores.
2. It appends a schema-versioned `KinexisEvent` to a Redis Stream partition.
3. A stream listener reads the event.
4. The processor upcasts old event envelopes before deserializing entity content.
5. The processor fans out to matching `EntityStore` beans in parallel.
6. Successful stores are marked processed using `{eventId, storeName}` idempotency.
7. Failed stores remain pending and are retried.
8. After retry exhaustion, Kinexis writes one DLQ record per failed store.

If MySQL succeeds, PostgreSQL fails, and the retry limit is reached, only PostgreSQL gets a DLQ record. A normal replay preserves the original `eventId`, so MySQL is not written again.

## Event Schema Evolution

Use event schema evolution when a deployed entity payload shape changes while Redis Streams or DLQ records may still contain older payloads.

```properties
kinexis.event-schema.enabled=true
kinexis.event-schema.current-version=2
kinexis.event-schema.entity-versions.com.example.Employer=2
```

Register one or more `KinexisEventUpcaster` beans. Kinexis calls them before processor deserialization and before DLQ replay appends the record back to the entity stream.

```java
import com.foogaro.kinexis.core.model.KinexisEventEnvelope;
import com.foogaro.kinexis.core.service.KinexisEventUpcaster;
import org.springframework.stereotype.Component;

@Component
public class EmployerV1ToV2Upcaster implements KinexisEventUpcaster {

    @Override
    public boolean supports(String entityType, String fromVersion, String toVersion) {
        return Employer.class.getName().equals(entityType)
                && "1".equals(fromVersion)
                && "2".equals(toVersion);
    }

    @Override
    public KinexisEventEnvelope upcast(KinexisEventEnvelope envelope, String toVersion) {
        return envelope
                .withContent(envelope.content().replace("\"legacyName\"", "\"name\""))
                .withSchemaVersion("2");
    }
}
```

New events are stamped with the current schema version. Old events are migrated through the registry; if no matching upcaster exists, processing fails and the normal pending retry and DLQ behavior applies.

## Store Health

Inject `KinexisStoreControl` when operators need to pause, resume, inspect, or manually open a circuit for one store:

```java
kinexisStoreControl.pause(Employer.class, "mysql");
kinexisStoreControl.resume(Employer.class, "mysql");
kinexisStoreControl.openCircuit(Employer.class, "postgresql");
kinexisStoreControl.status(Employer.class, "mysql");
```

Kinexis also opens a store circuit automatically after repeated failures. While a store is paused or its circuit is open, calls to that store are skipped and the normal pending retry and per-store DLQ flow applies. Store-targeted DLQ replay skips paused and open-circuit stores by default; use `KinexisReplayOptions.forced()` only when an operator explicitly wants to override the guardrail.

Add `StoreHealthCheck` when a store can be probed before the next write:

```java
import com.foogaro.kinexis.core.model.StoreHealthCheckResult;
import com.foogaro.kinexis.core.store.EntityStore;
import com.foogaro.kinexis.core.store.StoreHealthCheck;

final class MysqlEmployerStore
        implements EntityStore<Employer>, StoreHealthCheck {

    @Override
    public StoreHealthCheckResult check() {
        return repository.ping()
                ? StoreHealthCheckResult.up("ready")
                : StoreHealthCheckResult.unhealthy("mysql unavailable");
    }
}
```

If the check is a separate bean instead of part of the store, implement `checkedEntityType()` and `checkedStoreName()` so Kinexis can bind it to the right store. Spring auto-registers both patterns. Operators can call `kinexisStoreControl.checkStatus(Employer.class, "mysql")` or `kinexisStoreControl.checkAll()`; diagnostics use active checks when one is available and still preserve manual `PAUSED` state.

## DLQ Operations

Inject `KinexisDlqService` for operational replay:

```java
import com.foogaro.kinexis.core.model.KinexisDlqRecord;
import com.foogaro.kinexis.core.model.KinexisReplayOptions;
import com.foogaro.kinexis.core.model.KinexisReplayPlan;
import com.foogaro.kinexis.core.model.KinexisReplayResult;
import com.foogaro.kinexis.core.model.ReplayBatchOptions;
import com.foogaro.kinexis.core.service.KinexisDlqService;

import java.time.Duration;
import java.util.List;

List<KinexisDlqRecord> all = kinexisDlqService.list(Employer.class);

List<KinexisDlqRecord> mysqlFailures =
        kinexisDlqService.listByFailedStore(Employer.class, "mysql");

KinexisReplayPlan mysqlPlan =
        kinexisDlqService.previewReplayByStore(Employer.class, "mysql");

kinexisDlqService.replayFailedStore(Employer.class, dlqRecordId);
kinexisDlqService.replayByStore(Employer.class, "postgresql");
kinexisDlqService.replayAllFailedStores(Employer.class);

KinexisReplayResult result =
        kinexisDlqService.replayFailedStoreResult(Employer.class, dlqRecordId);

List<KinexisReplayResult> postgresResults =
        kinexisDlqService.replayByStoreIfHealthy(Employer.class, "postgresql");

List<KinexisReplayResult> boundedMysqlReplay =
        kinexisDlqService.replayByStore(
                Employer.class,
                "mysql",
                new ReplayBatchOptions(
                        100,
                        Duration.ofMinutes(30),
                        true,
                        true,
                        Duration.ofMillis(25),
                        KinexisReplayOptions.safe()
                )
        );

KinexisReplayResult forced =
        kinexisDlqService.replayFailedStoreResult(
                Employer.class,
                dlqRecordId,
                KinexisDlqService.ReplayMode.REPLAY_ONLY,
                KinexisReplayOptions.forced()
        );
```

`KinexisReplayPlan` is a dry run. It reports records found, target stores, stores already processed by idempotency, unhealthy stores, records that would be skipped, records that would be replayed, and records requiring schema upcast. It does not append anything to Redis.

Use `ReplayBatchOptions` for large DLQ recovery. It caps replay volume, filters records by age, can delete successfully replayed DLQ records, can stop after the first non-replayed result, and can add a delay between records to reduce store pressure.

Use `replayWithNewEventId(...)` only when you explicitly want the replay to be treated as new work by all matching stores.

## Telemetry

Kinexis exposes a lightweight `KinexisTelemetry` abstraction and uses `SimpleKinexisTelemetry` by default. If Micrometer and a `MeterRegistry` are present, Kinexis can bridge metrics without forcing Micrometer as a core dependency.

Useful metrics include:

| Metric | Meaning |
| --- | --- |
| `kinexis.stream.events.published` | Events appended to Redis Streams. |
| `kinexis.stream.events.processed` | Events processed successfully. |
| `kinexis.stream.events.upcasted` | Old stream or DLQ records migrated to the current schema version. |
| `kinexis.store.write.latency` | Store save/delete latency by store and operation. |
| `kinexis.store.failures` | Store failures by exception class. |
| `kinexis.store.health.state` | Current store health state gauge. |
| `kinexis.store.probe.failures` | Failed write probes or active health checks. |
| `kinexis.store.probe.successes` | Successful recovery probes or active health checks. |
| `kinexis.pending.retries` | Pending retry pressure. |
| `kinexis.dlq.records` | DLQ records by failed store. |
| `kinexis.dlq.replays` | Replay activity by failed store and event ID mode. |
| `kinexis.cache.hits` | Cache hits. |
| `kinexis.cache.misses` | Cache misses. |

## Minimal Checklist

Before running the app, verify:

1. Redis is available.
2. MySQL and PostgreSQL datasources are configured.
3. `@EnableScheduling` is enabled.
4. `KinexisConfiguration` is imported.
5. The entity has an ID field.
6. At least one `CacheStore<Employer>` bean exists for cache patterns.
7. At least one target `EntityStore<Employer>` bean exists for write-behind.
8. Store names are unique for the entity.
9. Target aliases match how you call `save(entity, "...")` and `delete(id, "...")`.

## Testing

For application tests, use Testcontainers for Redis, MySQL, and PostgreSQL. Useful cases:

1. `findById` returns from cache on hit.
2. `findById` loads from primary and refills cache on miss.
3. `save` publishes a stream event.
4. Stream processing writes to both MySQL and PostgreSQL.
5. Targeted save writes only to the selected store.
6. A failed store is retried and then moved to a per-store DLQ.
7. Replaying a failed store does not rewrite stores that already succeeded.

The repository demos under `demo/` show concrete Spring Boot applications for individual stores and a combined multi-datasource setup.
