package com.foogaro.kinexis.core.store;

import java.time.Duration;

public interface CacheStore<T> extends EntityStore<T> {

    default T save(T entity, Duration ttl) {
        return save(entity);
    }
}
