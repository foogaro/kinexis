package com.foogaro.kinexis.core.store;

import com.foogaro.kinexis.core.model.StoreHealthCheckResult;

import java.util.Optional;

public interface StoreHealthCheck {

    StoreHealthCheckResult check();

    default Optional<Class<?>> checkedEntityType() {
        return Optional.empty();
    }

    default Optional<String> checkedStoreName() {
        return Optional.empty();
    }
}
