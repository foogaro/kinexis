package com.foogaro.kinexis.core.processor;

import com.foogaro.kinexis.core.Misc;
import com.foogaro.kinexis.core.config.KinexisProperties;
import com.foogaro.kinexis.core.exception.KinexisBackpressureException;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class KinexisProcessingCoordinator {

    private static final String PROCESSED_KEY_PREFIX = "kinexis:processed";
    private static final String ORDERING_KEY_PREFIX = "kinexis:ordering";

    private final RedisTemplate<String, String> redisTemplate;
    private final KinexisProperties properties;
    private final KinexisProcessingMetrics metrics;
    private final ConcurrentMap<String, LockRef> entityLocks = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SemaphoreRef> streamSemaphores = new ConcurrentHashMap<>();

    public KinexisProcessingCoordinator(RedisTemplate<String, String> redisTemplate, KinexisProperties properties) {
        this(redisTemplate, properties, new KinexisProcessingMetrics());
    }

    public KinexisProcessingCoordinator(RedisTemplate<String, String> redisTemplate,
                                        KinexisProperties properties,
                                        KinexisProcessingMetrics metrics) {
        this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate cannot be null");
        this.properties = Objects.requireNonNull(properties, "properties cannot be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null");
    }

    public <R> R inStreamCapacity(String streamKey, ProcessingOperation<R> operation) throws Exception {
        int maxInFlight = properties.getProcessing().getBackpressure().getMaxInFlightPerStream();
        if (maxInFlight <= 0 || streamKey == null || streamKey.isBlank()) {
            return operation.execute();
        }
        SemaphoreRef semaphoreRef = streamSemaphores.compute(streamKey, (ignored, existing) -> {
            SemaphoreRef selected = existing == null ? new SemaphoreRef(maxInFlight) : existing;
            selected.users.incrementAndGet();
            return selected;
        });
        acquirePermit(streamKey, semaphoreRef.semaphore);
        try {
            return operation.execute();
        } finally {
            semaphoreRef.semaphore.release();
            streamSemaphores.computeIfPresent(streamKey, (ignored, existing) ->
                    existing == semaphoreRef && existing.users.decrementAndGet() == 0 ? null : existing);
        }
    }

    public <R> R inEntityOrder(Class<?> entityType, String entityId, ProcessingOperation<R> operation) throws Exception {
        if (!properties.getProcessing().getOrdering().isPerEntityEnabled() || entityId == null || entityId.isBlank()) {
            return operation.execute();
        }
        String lockKey = orderingKey(entityType, entityId);
        LockRef lockRef = entityLocks.compute(lockKey, (ignored, existing) -> {
            LockRef selected = existing == null ? new LockRef() : existing;
            selected.users.incrementAndGet();
            return selected;
        });
        lockRef.lock.lock();
        try {
            return operation.execute();
        } finally {
            lockRef.lock.unlock();
            entityLocks.computeIfPresent(lockKey, (ignored, existing) ->
                    existing == lockRef && existing.users.decrementAndGet() == 0 ? null : existing);
        }
    }

    private void acquirePermit(String streamKey, Semaphore semaphore) throws InterruptedException {
        KinexisProperties.QueueFullBehavior behavior = properties.getProcessing().getBackpressure().getQueueFullBehavior();
        if (behavior == KinexisProperties.QueueFullBehavior.REJECT_TO_DLQ) {
            if (!semaphore.tryAcquire()) {
                metrics.recordBackpressureRejection();
                throw new KinexisBackpressureException("Maximum in-flight records reached for stream " + streamKey);
            }
            return;
        }
        if (behavior == KinexisProperties.QueueFullBehavior.SLOW_DOWN) {
            Duration delay = properties.getProcessing().getBackpressure().getSlowDownDelay();
            long sleepMillis = delay == null || delay.isNegative() ? 0L : delay.toMillis();
            while (!semaphore.tryAcquire()) {
                metrics.recordBackpressureSlowdown();
                if (sleepMillis <= 0L) {
                    Thread.yield();
                } else {
                    Thread.sleep(sleepMillis);
                }
            }
            return;
        }
        semaphore.acquire();
    }

    public boolean isProcessed(Class<?> entityType, String eventId, String storeName) {
        if (!properties.getProcessing().getIdempotency().isEnabled()) {
            return false;
        }
        return Boolean.TRUE.equals(redisTemplate.hasKey(processedKey(entityType, eventId, storeName)));
    }

    public void markProcessed(Class<?> entityType, String eventId, String storeName) {
        if (!properties.getProcessing().getIdempotency().isEnabled()) {
            return;
        }
        Duration retention = properties.getProcessing().getIdempotency().getRetention();
        if (retention == null || retention.isZero() || retention.isNegative()) {
            redisTemplate.opsForValue().set(processedKey(entityType, eventId, storeName), "1");
        } else {
            redisTemplate.opsForValue().set(processedKey(entityType, eventId, storeName), "1", retention);
        }
    }

    private String processedKey(Class<?> entityType, String eventId, String storeName) {
        return PROCESSED_KEY_PREFIX
                + Misc.KEY_SEPARATOR + entityType.getName()
                + Misc.KEY_SEPARATOR + storeName
                + Misc.KEY_SEPARATOR + eventId;
    }

    private String orderingKey(Class<?> entityType, String entityId) {
        return ORDERING_KEY_PREFIX
                + Misc.KEY_SEPARATOR + entityType.getName()
                + Misc.KEY_SEPARATOR + entityId;
    }

    @FunctionalInterface
    public interface ProcessingOperation<R> {
        R execute() throws Exception;
    }

    private static final class LockRef {

        private final ReentrantLock lock = new ReentrantLock(true);
        private final AtomicInteger users = new AtomicInteger();
    }

    private static final class SemaphoreRef {

        private final Semaphore semaphore;
        private final AtomicInteger users = new AtomicInteger();

        private SemaphoreRef(int permits) {
            this.semaphore = new Semaphore(permits, true);
        }
    }
}
