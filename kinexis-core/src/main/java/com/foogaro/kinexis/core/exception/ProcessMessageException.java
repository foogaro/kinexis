package com.foogaro.kinexis.core.exception;

import java.util.Collection;
import java.util.List;

/**
 * Checked exception thrown when an error occurs during message processing.
 * This exception is used to indicate that a message could not be processed
 * successfully, allowing the system to handle the failure appropriately
 * (e.g., retrying the message or moving it to a dead letter queue).
 */
public class ProcessMessageException extends Exception {

    private final String failedStore;
    private final List<String> failedStores;

    /**
     * Constructs a new ProcessMessageException with no detail message.
     */
    public ProcessMessageException() {
        this.failedStore = null;
        this.failedStores = List.of();
    }

    /**
     * Constructs a new ProcessMessageException with the specified detail message.
     *
     * @param message the detail message (which is saved for later retrieval by the getMessage() method)
     */
    public ProcessMessageException(String message) {
        super(message);
        this.failedStore = null;
        this.failedStores = List.of();
    }

    /**
     * Constructs a new ProcessMessageException with the specified detail message and cause.
     *
     * @param message the detail message (which is saved for later retrieval by the getMessage() method)
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     */
    public ProcessMessageException(String message, Throwable cause) {
        super(message, cause);
        this.failedStore = null;
        this.failedStores = List.of();
    }

    public ProcessMessageException(String message, Throwable cause, String failedStore) {
        super(message, cause);
        this.failedStore = failedStore;
        this.failedStores = failedStore == null ? List.of() : List.of(failedStore);
    }

    public ProcessMessageException(String message, Throwable cause, Collection<String> failedStores) {
        super(message, cause);
        this.failedStores = failedStores == null ? List.of() : List.copyOf(failedStores);
        this.failedStore = this.failedStores.isEmpty() ? null : this.failedStores.getFirst();
    }

    /**
     * Constructs a new ProcessMessageException with the specified cause and a detail message
     * of (cause==null ? null : cause.toString()) (which typically contains the class and
     * detail message of cause).
     *
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     */
    public ProcessMessageException(Throwable cause) {
        super(cause);
        this.failedStore = null;
        this.failedStores = List.of();
    }

    /**
     * Constructs a new ProcessMessageException with the specified detail message,
     * cause, suppression enabled or disabled, and writable stack trace enabled or disabled.
     *
     * @param message the detail message
     * @param cause the cause
     * @param enableSuppression whether or not suppression is enabled or disabled
     * @param writableStackTrace whether or not the stack trace should be writable
     */
    public ProcessMessageException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.failedStore = null;
        this.failedStores = List.of();
    }

    public String getFailedStore() {
        return failedStore;
    }

    public List<String> getFailedStores() {
        return failedStores;
    }
}
