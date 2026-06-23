package com.foogaro.kinexis.core.store;

import java.util.Set;
import java.util.Optional;

public interface EntityStore<T> {

    String name();

    Class<T> entityType();

    default Set<String> targets() {
        return Set.of(name());
    }

    Optional<T> findById(Object id);

    T save(T entity);

    void deleteById(Object id);
}
