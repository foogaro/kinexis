package com.foogaro.kinexis.core.store;

import java.util.List;
import java.util.Optional;

public class EmptyEntityStoreRegistry implements EntityStoreRegistry {

    @Override
    public <T> Optional<CacheStore<T>> findCacheStore(Class<T> entityType) {
        return Optional.empty();
    }

    @Override
    public <T> Optional<EntityStore<T>> findPrimaryStore(Class<T> entityType) {
        return Optional.empty();
    }

    @Override
    public <T> List<EntityStore<T>> findTargetStores(Class<T> entityType) {
        return List.of();
    }
}
