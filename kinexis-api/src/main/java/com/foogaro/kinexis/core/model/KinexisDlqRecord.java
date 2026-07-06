package com.foogaro.kinexis.core.model;

import java.time.Instant;
import java.util.List;

public record KinexisDlqRecord(
        String recordId,
        String eventId,
        String entityType,
        String entityId,
        String operation,
        String failedStore,
        int attempts,
        String reason,
        String exceptionClass,
        Instant failureTimestamp,
        List<String> targets) {
}
