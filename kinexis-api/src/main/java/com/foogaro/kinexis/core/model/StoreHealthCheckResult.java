package com.foogaro.kinexis.core.model;

import java.util.Objects;

public record StoreHealthCheckResult(
        boolean healthy,
        boolean skipped,
        String reason,
        String exceptionClass) {

    public StoreHealthCheckResult {
        reason = Objects.toString(reason, "");
        exceptionClass = Objects.toString(exceptionClass, "");
    }

    public static StoreHealthCheckResult up() {
        return new StoreHealthCheckResult(true, false, "", "");
    }

    public static StoreHealthCheckResult up(String reason) {
        return new StoreHealthCheckResult(true, false, reason, "");
    }

    public static StoreHealthCheckResult unhealthy(String reason) {
        return new StoreHealthCheckResult(false, false, reason, "");
    }

    public static StoreHealthCheckResult skipped(String reason) {
        return new StoreHealthCheckResult(false, true, reason, "");
    }

    public static StoreHealthCheckResult failed(Throwable throwable) {
        if (throwable == null) {
            return unhealthy("");
        }
        return new StoreHealthCheckResult(
                false,
                false,
                Objects.toString(throwable.getMessage(), ""),
                throwable.getClass().getName());
    }
}
