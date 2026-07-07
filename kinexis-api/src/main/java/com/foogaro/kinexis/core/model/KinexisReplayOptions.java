package com.foogaro.kinexis.core.model;

public record KinexisReplayOptions(boolean force) {

    public static KinexisReplayOptions safe() {
        return new KinexisReplayOptions(false);
    }

    public static KinexisReplayOptions forced() {
        return new KinexisReplayOptions(true);
    }
}
