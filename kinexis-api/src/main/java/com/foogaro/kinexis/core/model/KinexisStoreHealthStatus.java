package com.foogaro.kinexis.core.model;

import java.time.Instant;

public record KinexisStoreHealthStatus(
        String entityType,
        String storeName,
        KinexisStoreHealthState state,
        int failures,
        int probeSuccesses,
        Instant openedUntil,
        Instant lastFailureAt) {

    public boolean available() {
        return state == KinexisStoreHealthState.ACTIVE
                || state == KinexisStoreHealthState.DEGRADED
                || state == KinexisStoreHealthState.RECOVERING;
    }
}
