package com.foogaro.kinexis.core.model;

import java.time.Duration;

public record ReplayBatchOptions(
        int limit,
        Duration olderThan,
        boolean deleteAfterReplay,
        boolean stopOnFirstFailure,
        Duration delayBetweenRecords,
        KinexisReplayOptions replayOptions) {

    public ReplayBatchOptions {
        limit = limit <= 0 ? Integer.MAX_VALUE : limit;
        delayBetweenRecords = delayBetweenRecords == null || delayBetweenRecords.isNegative()
                ? Duration.ZERO
                : delayBetweenRecords;
        replayOptions = replayOptions == null ? KinexisReplayOptions.safe() : replayOptions;
    }

    public static ReplayBatchOptions defaults() {
        return new ReplayBatchOptions(
                Integer.MAX_VALUE,
                null,
                false,
                false,
                Duration.ZERO,
                KinexisReplayOptions.safe());
    }

    public static ReplayBatchOptions limited(int limit) {
        return new ReplayBatchOptions(
                limit,
                null,
                false,
                false,
                Duration.ZERO,
                KinexisReplayOptions.safe());
    }
}
