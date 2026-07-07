# Kinexis Workflows

This document describes the main Kinexis runtime flows using Mermaid diagrams.

## Service Entry Flow

```mermaid
graph TD
    App["Application Service / Controller"] --> KS["KinexisService"]

    KS --> Enabled{"@CachingPatterns.enabled?"}

    Enabled -- "false" --> PrimaryOnly["Delegate directly to primary EntityStore"]
    PrimaryOnly --> DB["Primary backing store"]

    Enabled -- "true" --> Operation{"Operation"}

    Operation -- "findById" --> CacheRead["Read CacheStore"]
    CacheRead --> CacheHit{"Cache hit?"}
    CacheHit -- "yes" --> ReturnEntity["Return entity"]
    CacheHit -- "no" --> CachePattern{"CACHE_ASIDE or REFRESH_AHEAD?"}
    CachePattern -- "yes" --> PrimaryRead["Read primary EntityStore"]
    PrimaryRead --> CacheWrite["Write CacheStore with optional TTL"]
    CacheWrite --> ReturnEntity
    CachePattern -- "no" --> Empty["Return empty"]

    Operation -- "save/update/delete" --> WriteBehind{"WRITE_BEHIND enabled?"}
    WriteBehind -- "no" --> CacheDirect["Write/delete CacheStore"]
    CacheDirect --> Done["Done"]

    WriteBehind -- "yes" --> ValidateTargets["Validate target EntityStore beans"]
    ValidateTargets --> BuildEvent["Build KinexisEvent - operation, eventId, entityType, entityId, targets, content"]
    BuildEvent --> Partition["StreamPartitioner routes by entityId"]
    Partition --> RedisStream["Redis Stream partition - wb:stream:entity:entity:partition:n"]
    RedisStream --> Done
```

## Write-Behind Processing Flow

```mermaid
graph TD
    RedisStream["Redis Stream partition"] --> Listener["Generated / runtime stream listener"]
    Listener --> Processor["AbstractProcessor"]

    Processor --> Capacity["Check max in-flight per stream"]
    Capacity --> EntityOrder["Serialize local processing per entityId"]
    EntityOrder --> ResolveStores["EntityStoreRegistry.findTargetStores(entity, targets)"]

    ResolveStores --> FanOut["Fan out to target stores in parallel"]

    FanOut --> StoreA["PostgreSQL EntityStore"]
    FanOut --> StoreB["MySQL EntityStore"]
    FanOut --> StoreC["MongoDB EntityStore"]
    FanOut --> StoreD["SQL Server EntityStore"]
    FanOut --> StoreE["Cassandra EntityStore"]

    StoreA --> IdemA{"eventId + store already processed?"}
    StoreB --> IdemB{"eventId + store already processed?"}
    StoreC --> IdemC{"eventId + store already processed?"}
    StoreD --> IdemD{"eventId + store already processed?"}
    StoreE --> IdemE{"eventId + store already processed?"}

    IdemA -- "yes" --> SkipA["Skip"]
    IdemB -- "yes" --> SkipB["Skip"]
    IdemC -- "yes" --> SkipC["Skip"]
    IdemD -- "yes" --> SkipD["Skip"]
    IdemE -- "yes" --> SkipE["Skip"]

    IdemA -- "no" --> HealthA{"Store active or recovering?"}
    IdemB -- "no" --> HealthB{"Store active or recovering?"}
    IdemC -- "no" --> HealthC{"Store active or recovering?"}
    IdemD -- "no" --> HealthD{"Store active or recovering?"}
    IdemE -- "no" --> HealthE{"Store active or recovering?"}

    HealthA -- "yes" --> WriteA["save/delete"]
    HealthB -- "yes" --> WriteB["save/delete"]
    HealthC -- "yes" --> WriteC["save/delete"]
    HealthD -- "yes" --> WriteD["save/delete"]
    HealthE -- "yes" --> WriteE["save/delete"]

    HealthA -- "paused/open" --> HealthFailA["Store failure"]
    HealthB -- "paused/open" --> HealthFailB["Store failure"]
    HealthC -- "paused/open" --> HealthFailC["Store failure"]
    HealthD -- "paused/open" --> HealthFailD["Store failure"]
    HealthE -- "paused/open" --> HealthFailE["Store failure"]

    WriteA --> MarkA["Mark processed"]
    WriteB --> MarkB["Mark processed"]
    WriteC --> MarkC["Mark processed"]
    WriteD --> MarkD["Mark processed"]
    WriteE --> MarkE["Mark processed"]

    MarkA --> Result{"Any store failed?"}
    MarkB --> Result
    MarkC --> Result
    MarkD --> Result
    MarkE --> Result
    SkipA --> Result
    SkipB --> Result
    SkipC --> Result
    SkipD --> Result
    SkipE --> Result
    HealthFailA --> Result
    HealthFailB --> Result
    HealthFailC --> Result
    HealthFailD --> Result
    HealthFailE --> Result

    Result -- "no" --> Ack["XACK stream record"]
    Ack --> MetricsOK["Telemetry: processed, latency, queue gauges"]

    Result -- "yes" --> NoAck["Do not ack"]
    NoAck --> PEL["Redis Pending Entries List"]
    PEL --> MetricsFail["Telemetry: store failures"]
```

## Store Health Flow

```mermaid
graph TD
    Failure["Store call failure"] --> Count["Record failure in rolling window"]
    Count --> Threshold{"Failures >= threshold?"}
    Threshold -- "no" --> Degraded["State DEGRADED"]
    Threshold -- "yes" --> Open["State OPEN_CIRCUIT"]

    Open --> Wait["Skip store calls until open-duration elapses"]
    Wait --> Recovering["State RECOVERING"]
    Recovering --> Probe["Allow probe store calls"]
    Probe --> ProbeResult{"Probe success?"}

    ProbeResult -- "yes" --> ProbeCount["Increment probe successes"]
    ProbeCount --> Close{"Successes >= probe-success-threshold?"}
    Close -- "yes" --> Active["State ACTIVE"]
    Close -- "no" --> Recovering

    ProbeResult -- "no" --> Open

    Operator["Operator"] --> Pause["pause(entity, store)"]
    Pause --> Paused["State PAUSED"]
    Paused --> Skipped["Skip store calls into retry/DLQ path"]
    Operator --> Resume["resume(entity, store)"]
    Resume --> Active
```

## Pending Retry And Per-Store DLQ Flow

```mermaid
graph TD
    PEL["Redis Pending Entries List"] --> PendingScheduler["AbstractPendingMessageHandler - scheduled by listener.pending.fixed-delay"]

    PendingScheduler --> Scan["Scan pending records - batch-size"]
    Scan --> RetryCounter["Increment retry counter - TTL = max-retention"]

    RetryCounter --> Attempts{"attempts >= max-attempts?"}

    Attempts -- "no" --> Reprocess["Call Processor.process(record)"]
    Reprocess --> RetryOK{"Success?"}
    RetryOK -- "yes" --> Ack["Acknowledge record"]
    Ack --> ClearCounter["Expire retry counter"]
    RetryOK -- "no" --> RemainPending["Leave in PEL for later retry"]

    Attempts -- "yes" --> SplitFailures["Extract failed store names from ProcessMessageException"]

    SplitFailures --> DLQMySQL["DLQ record - failedStore=mysql"]
    SplitFailures --> DLQSQL["DLQ record - failedStore=sqlserver"]

    DLQMySQL --> DLQStream["Redis DLQ Stream - wb:stream:entity:entity:dlq"]
    DLQSQL --> DLQStream

    DLQStream --> Metadata["KinexisDlqRecord - recordId, eventId, entityType, entityId, - operation, failedStore, attempts, - reason, exceptionClass, failureTimestamp, targets"]

    Metadata --> DeleteOriginal["Delete / acknowledge exhausted pending record"]
    DeleteOriginal --> TelemetryDLQ["Telemetry: dlq.records, pending.retries"]
```

## DLQ Listing And Replay Flow

```mermaid
graph TD
    Operator["Operator / job / application code"] --> DlqService["KinexisDlqService"]

    DlqService --> Query{"DLQ action"}

    Query -- "list(entity)" --> ListAll["Return List of KinexisDlqRecord"]
    Query -- "listByFailedStore(entity, store)" --> ListStore["Filter by failedStore"]
    Query -- "listByOperation(entity, operation)" --> ListOperation["Filter by operation"]
    Query -- "listOlderThan(entity, age)" --> ListAge["Filter by failureTimestamp"]

    Query -- "replayFailedStore(entity, recordId)" --> ReplayFailed["Read DLQ record - use failedStore as target"]
    Query -- "replayByStore(entity, mysql)" --> ReplayStore["Replay all DLQ records where failedStore=mysql"]
    Query -- "replayAllFailedStores(entity)" --> ReplayAll["Replay every store-specific DLQ record"]
    Query -- "replayByStoreIfHealthy(...)" --> ReplayHealthy["Return KinexisReplayResult for replayed or skipped records"]
    Query -- "replayWithNewEventId(...)" --> NewEvent["Generate new eventId"]

    ReplayFailed --> HealthCheck{"failedStore healthy or force option?"}
    ReplayStore --> HealthCheck
    ReplayAll --> HealthCheck
    ReplayHealthy --> HealthCheck

    HealthCheck -- "paused/open and not forced" --> Skipped["KinexisReplayResult - SKIPPED_UNHEALTHY_STORE"]
    HealthCheck -- "healthy or forced" --> Preserve{"Normal replay?"}

    Preserve -- "yes" --> SameEvent["Preserve original eventId"]
    Preserve -- "no" --> NewEvent

    SameEvent --> AppendReplay["Append replay message to original stream"]
    NewEvent --> AppendReplay

    AppendReplay --> Processor["Processor consumes replayed event"]

    Processor --> Idempotency["Store-level idempotency"]
    Idempotency --> SkipSucceeded["Already-successful stores are skipped"]
    Idempotency --> RetryFailedOnly["Failed store is retried"]

    AppendReplay --> ReplayTelemetry["Telemetry: dlq.replays - failedStore, targets, eventIdMode"]
    Skipped --> ReplayFailure
    DlqService --> ReplayFailure["Telemetry: dlq.replay.failures"]
```

## Module Boundary Flow

```mermaid
graph LR
    subgraph API["kinexis-api"]
        Ann["@CachingPatterns"]
        Event["KinexisEvent"]
        Stores["EntityStore / CacheStore"]
        Registry["EntityStoreRegistry"]
        Telemetry["KinexisTelemetry"]
        Props["KinexisProperties"]
    end

    subgraph Spring["kinexis-spring"]
        Service["KinexisService"]
        StoreAdapters["CrudRepositoryEntityStore - CrudRepositoryCacheStore"]
        SpringRegistry["BeanFinderEntityStoreRegistry"]
        Micrometer["Optional Micrometer bridge"]
    end

    subgraph RedisStreams["kinexis-redis-streams"]
        Publisher["RedisStreamEventPublisher"]
        Listener["AbstractStreamListener"]
        Processor["AbstractProcessor"]
        Pending["AbstractPendingMessageHandler"]
        DLQ["KinexisDlqService / KinexisDlqWriter"]
        Backpressure["KinexisStoreExecutor - KinexisProcessingCoordinator"]
        Partitioner["StreamPartitioner"]
    end

    subgraph RedisOM["kinexis-redis-om"]
        AnnotationProcessor["CachingPatternsAnnotationProcessor"]
        RedisOmStore["RedisOmCacheStore - TTL-aware"]
    end

    App["Application / demo projects"] --> Service
    Service --> Ann
    Service --> Registry
    Service --> Publisher
    Registry --> Stores

    Publisher --> Partitioner
    Listener --> Processor
    Processor --> Stores
    Processor --> Backpressure
    Pending --> Processor
    Pending --> DLQ
    DLQ --> Publisher

    RedisOmStore --> Stores
    StoreAdapters --> Stores
    Micrometer --> Telemetry
    Processor --> Telemetry
    Pending --> Telemetry
    DLQ --> Telemetry
```

## Write-Behind Failure Sequence

```mermaid
sequenceDiagram
    participant App as Application
    participant KS as KinexisService
    participant Reg as EntityStoreRegistry
    participant Pub as RedisStreamEventPublisher
    participant RS as Redis Streams
    participant L as Stream Listener
    participant P as Processor
    participant S1 as PostgreSQL Store
    participant S2 as MySQL Store
    participant S3 as MongoDB Store
    participant DLQ as DLQ Stream

    App->>KS: save(entity, targets)
    KS->>Reg: validate target stores
    Reg-->>KS: matching EntityStore beans
    KS->>Pub: append KinexisEvent
    Pub->>RS: XADD partitioned stream

    RS-->>L: deliver record
    L->>P: process(record)
    P->>Reg: findTargetStores(entity, targets)
    Reg-->>P: PostgreSQL, MySQL, MongoDB

    par fan-out
        P->>S1: save/delete
        S1-->>P: success
    and
        P->>S2: save/delete
        S2-->>P: failure
    and
        P->>S3: save/delete
        S3-->>P: success
    end

    P->>P: mark processed for successful stores
    P-->>L: ProcessMessageException(failedStores=[mysql])
    L-->>RS: record remains pending

    Note over RS,L: Pending handler retries until max-attempts

    L->>DLQ: XADD failedStore=mysql with metadata
    L->>RS: cleanup exhausted pending record
```

## Cache-Aside And TTL Flow

```mermaid
graph TD
    Read["findById(id)"] --> Cache["CacheStore.findById(id)"]
    Cache --> Hit{"Found?"}

    Hit -- "yes" --> HitMetric["kinexis.cache.hits"]
    HitMetric --> Return["Return cached entity"]

    Hit -- "no" --> MissMetric["kinexis.cache.misses"]
    MissMetric --> Primary["Primary EntityStore.findById(id)"]

    Primary --> FoundPrimary{"Found in primary?"}
    FoundPrimary -- "no" --> Empty["Return Optional.empty"]

    FoundPrimary -- "yes" --> TTL{"@CachingPatterns.ttl > 0?"}
    TTL -- "yes" --> SaveTTL["CacheStore.save(entity, ttl)"]
    TTL -- "no" --> SaveCache["CacheStore.save(entity)"]

    SaveTTL --> Return
    SaveCache --> Return

    SaveTTL --> RedisOM{"RedisOmCacheStore?"}
    RedisOM -- "yes" --> ExpireKey["Apply Redis key expiration"]
    RedisOM -- "no" --> IgnoreTTL["Store may ignore TTL safely"]
```
