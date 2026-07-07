package com.foogaro.kinexis.core.model;

public record KinexisReplayResult(
        String dlqRecordId,
        String replayedRecordId,
        String failedStore,
        KinexisReplayStatus status,
        String reason) {

    public static KinexisReplayResult replayed(String dlqRecordId, String replayedRecordId, String failedStore) {
        return new KinexisReplayResult(dlqRecordId, replayedRecordId, failedStore, KinexisReplayStatus.REPLAYED, null);
    }

    public static KinexisReplayResult skipped(String dlqRecordId, String failedStore, String reason) {
        return new KinexisReplayResult(dlqRecordId, null, failedStore, KinexisReplayStatus.SKIPPED_UNHEALTHY_STORE, reason);
    }

    public static KinexisReplayResult notFound(String dlqRecordId) {
        return new KinexisReplayResult(dlqRecordId, null, null, KinexisReplayStatus.NOT_FOUND, "notFound");
    }

    public static KinexisReplayResult failed(String dlqRecordId, String failedStore, String reason) {
        return new KinexisReplayResult(dlqRecordId, null, failedStore, KinexisReplayStatus.FAILED, reason);
    }
}
