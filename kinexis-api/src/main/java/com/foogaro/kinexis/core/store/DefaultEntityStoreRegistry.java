package com.foogaro.kinexis.core.store;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class DefaultEntityStoreRegistry implements EntityStoreRegistry {

    private final List<EntityStore<?>> explicitStores;
    private final EntityStoreRegistry fallbackRegistry;

    public DefaultEntityStoreRegistry(Collection<EntityStore<?>> explicitStores, EntityStoreRegistry fallbackRegistry) {
        this.explicitStores = List.copyOf(explicitStores);
        this.fallbackRegistry = fallbackRegistry;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<CacheStore<T>> findCacheStore(Class<T> entityType) {
        Optional<CacheStore<T>> explicit = explicitStores.stream()
                .filter(store -> store instanceof CacheStore<?>)
                .filter(store -> store.entityType().equals(entityType))
                .findFirst()
                .map(store -> (CacheStore<T>) store);
        return explicit.or(() -> fallbackRegistry.findCacheStore(entityType));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<EntityStore<T>> findPrimaryStore(Class<T> entityType) {
        Optional<EntityStore<T>> explicit = explicitStores.stream()
                .filter(store -> !(store instanceof CacheStore<?>))
                .filter(store -> store.entityType().equals(entityType))
                .findFirst()
                .map(store -> (EntityStore<T>) store);
        return explicit.or(() -> fallbackRegistry.findPrimaryStore(entityType));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<EntityStore<T>> findTargetStores(Class<T> entityType) {
        List<EntityStore<T>> explicit = explicitStores.stream()
                .filter(store -> store.entityType().equals(entityType))
                .filter(store -> !(store instanceof CacheStore<?>))
                .map(store -> (EntityStore<T>) store)
                .toList();
        return explicit.isEmpty() ? fallbackRegistry.findTargetStores(entityType) : explicit;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<EntityStore<T>> findTargetStores(Class<T> entityType, Collection<String> targets) {
        if (targets == null || targets.isEmpty()) {
            return findTargetStores(entityType);
        }
        List<EntityStore<T>> explicit = explicitStores.stream()
                .filter(store -> store.entityType().equals(entityType))
                .filter(store -> !(store instanceof CacheStore<?>))
                .filter(store -> store.targets().stream().anyMatch(targets::contains))
                .map(store -> (EntityStore<T>) store)
                .toList();
        return explicit.isEmpty() ? fallbackRegistry.findTargetStores(entityType, targets) : explicit;
    }
}
