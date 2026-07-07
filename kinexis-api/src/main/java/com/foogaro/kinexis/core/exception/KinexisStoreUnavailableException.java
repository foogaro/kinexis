package com.foogaro.kinexis.core.exception;

public class KinexisStoreUnavailableException extends RuntimeException {

    private final String storeName;

    public KinexisStoreUnavailableException(String storeName, String message) {
        super(message);
        this.storeName = storeName;
    }

    public KinexisStoreUnavailableException(String storeName, String message, Throwable cause) {
        super(message, cause);
        this.storeName = storeName;
    }

    public String getStoreName() {
        return storeName;
    }
}
