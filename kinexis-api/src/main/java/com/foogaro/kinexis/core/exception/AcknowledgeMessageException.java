package com.foogaro.kinexis.core.exception;

/**
 * Checked exception thrown when an error occurs during message acknowledgment.
 * This exception is used to indicate that a message could not be acknowledged
 * successfully after processing, which may require special handling such as
 * retrying the acknowledgment or moving the message to a dead letter queue.
 */
public class AcknowledgeMessageException extends Exception {

    /**
     * Constructs a new AcknowledgeMessageException with no detail message.
     */
    public AcknowledgeMessageException() {
    }

    /**
     * Constructs a new AcknowledgeMessageException with the specified detail message.
     *
     * @param message the detail message (which is saved for later retrieval by the getMessage() method)
     */
    public AcknowledgeMessageException(String message) {
        super(message);
    }

    /**
     * Constructs a new AcknowledgeMessageException with the specified detail message and cause.
     *
     * @param message the detail message (which is saved for later retrieval by the getMessage() method)
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     */
    public AcknowledgeMessageException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new AcknowledgeMessageException with the specified cause and a detail message
     * of (cause==null ? null : cause.toString()) (which typically contains the class and
     * detail message of cause).
     *
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     */
    public AcknowledgeMessageException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new AcknowledgeMessageException with the specified detail message,
     * cause, suppression enabled or disabled, and writable stack trace enabled or disabled.
     *
     * @param message the detail message
     * @param cause the cause
     * @param enableSuppression whether or not suppression is enabled or disabled
     * @param writableStackTrace whether or not the stack trace should be writable
     */
    public AcknowledgeMessageException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
