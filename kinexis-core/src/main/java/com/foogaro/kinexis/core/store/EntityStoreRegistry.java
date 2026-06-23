package com.foogaro.kinexis.core.store;

import java.util.List;
import java.util.Collection;
import java.util.Optional;

public interface EntityStoreRegistry {

    <T> Optional<CacheStore<T>> findCacheStore(Class<T> entityType);

    <T> Optional<EntityStore<T>> findPrimaryStore(Class<T> entityType);

    default <T> List<EntityStore<T>> findTargetStores(Class<T> entityType) {
        return List.of();
    }

    default <T> List<EntityStore<T>> findTargetStores(Class<T> entityType, Collection<String> targets) {
        return findTargetStores(entityType);
    }

    default <T, R> List<EntityStore<T>> findTargetStores(Class<T> entityType, Class<R> repositoryType) {
        return findTargetStores(entityType);
    }

    default <T, R> List<EntityStore<T>> findTargetStores(Class<T> entityType, Class<R> repositoryType, Collection<String> targets) {
        if (targets == null || targets.isEmpty()) {
            return findTargetStores(entityType, repositoryType);
        }
        return findTargetStores(entityType, targets);
    }
}
