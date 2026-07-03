package com.foogaro.kinexis.core.exception;

public class KinexisBackpressureException extends RuntimeException {

    public KinexisBackpressureException(String message) {
        super(message);
    }

    public KinexisBackpressureException(String message, Throwable cause) {
        super(message, cause);
    }
}
