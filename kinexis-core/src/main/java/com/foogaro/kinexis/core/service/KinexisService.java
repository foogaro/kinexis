package com.foogaro.kinexis.core.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.foogaro.kinexis.core.model.KinexisEvent;
import com.foogaro.kinexis.core.store.CacheStore;
import com.foogaro.kinexis.core.store.EntityStoreRegistry;
import com.foogaro.kinexis.core.stream.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.ParameterizedType;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Abstract base class for Kinexis services that handle entity operations with Redis caching.
 * This class provides core functionality for CRUD operations with support for various caching patterns
 * including Write-Behind, Cache-Aside, and Refresh-Ahead. It manages both Redis cache and database
 * operations through appropriate repositories.
 *
 * @param <T> the type of entity that this service handles
 */
public abstract class KinexisService<T> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Class<T> entityClass;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private AnnotationFinder annotationFinder;
    @Autowired
    private EntityStoreRegistry entityStoreRegistry;
    @Autowired
    private EventPublisher eventPublisher;

    /**
     * No-args constructor for KinexisService.
     * Initializes the entity class and stream key using reflection.
     */
    @SuppressWarnings("unchecked")
    public KinexisService() {
        this.entityClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    public Class<T> getEntityClass() {
        return entityClass;
    }

    /**
     * Saves an entity to the cache or initiates a write-behind operation.
     * If write-behind is enabled for the entity type, the operation is queued for asynchronous processing.
     * Otherwise, the entity is written directly to the cache.
     *
     * @param entity the entity to save
     */
    public void save(T entity) {
        save(entity, new String[0]);
    }

    public void save(T entity, String... targets) {
        if (Objects.nonNull(entity)) {
            if (!annotationFinder.isEnabled(entityClass)) {
                writeToDatabase(entity);
                logger.debug("Kinexis disabled for Entity {}", entityClass.getSimpleName());
            } else if (annotationFinder.hasWriteBehind(entityClass)) {
                writeBehindForInsert(entity, targets);
            } else {
                writeToCache(entity);
                logger.debug("Pattern WriteBehind not enabled for Entity {}", entityClass.getSimpleName());
            }
        }
    }

    /**
     * Updates an entity to the cache or initiates a write-behind operation.
     * If write-behind is enabled for the entity type, the operation is queued for asynchronous processing.
     * Otherwise, the entity is written directly to the cache.
     *
     * @param entity the entity to update
     */
    public void update(T entity) {
        save(entity);
    }

    /**
     * Finds an entity by its identifier.
     * This method implements a combination of Cache-Aside and Refresh-Ahead patterns:
     * 1. First attempts to read from cache
     * 2. If not found and Cache-Aside or Refresh-Ahead is enabled, loads from database
     * 3. Updates cache with the loaded entity
     *
     * @param id the identifier of the entity to find
     * @return an Optional containing the found entity
     */
    public Optional<T> findById(Object id) {
        Optional<T> entity = Optional.empty();
        if (Objects.nonNull(id)) {
            if (!annotationFinder.isEnabled(entityClass)) {
                logger.debug("Kinexis disabled for Entity {}", entityClass.getSimpleName());
                return readFromDatabase(id);
            }
            entity = readFromCache(id);
            if (entity.isPresent()) {
                return entity;
            } else {
                if (annotationFinder.hasCacheAside(entityClass) || annotationFinder.hasRefreshAhead(entityClass)) {
                    entity = cacheAside(id);
                    return entity;
                } else {
                    logger.debug("Pattern CacheAside not enabled for Entity {}", entityClass.getSimpleName());
                }
            }
        }
        return entity;
    }

    /**
     * Deletes an entity by its identifier.
     * If write-behind is enabled for the entity type, the deletion is queued for asynchronous processing.
     * Otherwise, the entity is deleted directly from the cache.
     *
     * @param id the identifier of the entity to delete
     */
    public void delete(Object id) {
        delete(id, new String[0]);
    }

    public void delete(Object id, String... targets) {
        if (Objects.nonNull(id)) {
            if (!annotationFinder.isEnabled(entityClass)) {
                deleteFromDatabase(id);
                logger.debug("Kinexis disabled for Entity {}", entityClass.getSimpleName());
            } else if (annotationFinder.hasWriteBehind(entityClass)) {
                writeBehindForDelete(id, targets);
                logger.debug("Deleted by Id: {}", id);
            } else {
                deleteFromCache(id);
                logger.debug("Pattern WriteBehind not enabled for Entity {}", entityClass.getSimpleName());
            }
        }
    }


    private String writeBehindForInsert(T entity, String... targets) {
        try {
            validateWriteBehindTargets(targets);
            String json = objectMapper.writeValueAsString(entity);
            String recordId = eventPublisher.append(entityClass, KinexisEvent.save(entityClass, json, targets));
            logger.debug("RecordId {} added for ingestion to the Stream for entity {}", recordId, entityClass.getSimpleName());
            return recordId;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeBehindForDelete(Object id, String... targets) {
        validateWriteBehindTargets(targets);
        String recordId = eventPublisher.append(entityClass, KinexisEvent.delete(entityClass, id, targets));
        logger.debug("RecordId {} added for deletion to the Stream for entity {}", Objects.nonNull(recordId) ? recordId : "<null>", entityClass.getSimpleName());
    }

    private void validateWriteBehindTargets(String... targets) {
        List<String> selectedTargets = Arrays.stream(targets == null ? new String[0] : targets)
                .filter(Objects::nonNull)
                .filter(target -> !target.isBlank())
                .toList();
        if (selectedTargets.isEmpty()) {
            if (entityStoreRegistry.findTargetStores(entityClass).isEmpty()) {
                throw new IllegalStateException("No target EntityStore configured for entity " + entityClass.getName());
            }
            return;
        }
        if (entityStoreRegistry.findTargetStores(entityClass, selectedTargets).isEmpty()) {
            throw new IllegalArgumentException("No target EntityStore configured for entity "
                    + entityClass.getName() + " and targets " + selectedTargets);
        }
    }

    private Optional<T> cacheAside(Object id) {
        Optional<T> entity = Optional.empty();
        if (Objects.nonNull(id)) {
            entity = readFromDatabase(id);
            if (entity.isPresent()) {
                entity = writeToCache(entity.get());
            } else {
                logger.debug("Entity not found in Database: {}", id);
            }
        } else {
            logger.warn("Id is null for entity {}", entityClass.getSimpleName());
        }
        return entity;
    }

    private Optional<T> readFromCache(Object id) {
        Optional<T> entity = entityStoreRegistry.findCacheStore(entityClass)
                .flatMap(store -> store.findById(id));
        entity.ifPresent(value -> logger.debug("Entity read from cache: {}", value));
        return entity;
    }

    private void deleteFromCache(Object id) {
        if (Objects.nonNull(id)) {
            Optional<CacheStore<T>> cacheStore = entityStoreRegistry.findCacheStore(entityClass);
            cacheStore.ifPresent(store -> store.deleteById(id));
            cacheStore.ifPresent(store -> logger.debug("Entity deleted from cache: {}", id));
        } else {
            logger.warn("Id is null for entity {}", entityClass.getSimpleName());
        }
    }

    private Optional<T> writeToCache(T entity) {
        if (Objects.nonNull(entity)) {
            Duration ttl = cacheTtl();
            Optional<T> savedEntity = entityStoreRegistry.findCacheStore(entityClass)
                    .map(store -> ttl.isZero() ? store.save(entity) : store.save(entity, ttl));
            savedEntity.ifPresent(value -> logger.debug("Entity written to cache: {}", value));
            return savedEntity;
        } else {
            logger.warn("Entity is null: {}", entity);
        }
        return Optional.empty();
    }

    private Optional<T> readFromDatabase(Object id) {
        Optional<T> entity = entityStoreRegistry.findPrimaryStore(entityClass)
                .flatMap(store -> store.findById(id));
        entity.ifPresent(value -> logger.debug("Entity read from database: {}", value));
        return entity;
    }

    private void deleteFromDatabase(Object id) {
        if (Objects.nonNull(id)) {
            entityStoreRegistry.findPrimaryStore(entityClass)
                    .ifPresent(store -> store.deleteById(id));
            logger.debug("Entity deleted from database: {}", id);
        } else {
            logger.warn("Id is null for entity {}", entityClass.getSimpleName());
        }
    }

    private Optional<T> writeToDatabase(T entity) {
        if (Objects.nonNull(entity)) {
            Optional<T> savedEntity = entityStoreRegistry.findPrimaryStore(entityClass)
                    .map(store -> store.save(entity));
            savedEntity.ifPresent(value -> logger.debug("Entity written to database: {}", value));
            return savedEntity;
        } else {
            logger.warn("Entity is null: {}", entity);
        }
        return Optional.empty();
    }

    private Duration cacheTtl() {
        long ttl = annotationFinder.ttl(entityClass);
        return ttl > 0 ? Duration.ofSeconds(ttl) : Duration.ZERO;
    }

}
