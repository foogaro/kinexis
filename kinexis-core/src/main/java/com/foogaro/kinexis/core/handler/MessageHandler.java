package com.foogaro.kinexis.core.handler;

import com.foogaro.kinexis.core.processor.Processor;

/**
 * Interface for components that handle message processing in the system.
 * This interface defines the contract for components that need to provide
 * a processor for handling specific types of messages.
 *
 * @param <T> the type of entity that this handler processes
 * @param <R> the type of repository used for entity operations
 */
public interface MessageHandler<T, R> {

    /**
     * Gets the processor that handles the actual message processing logic.
     * The processor is responsible for implementing the specific business logic
     * for handling messages of type T.
     *
     * @return the Processor instance that handles message processing
     */
    Processor<T, R> getProcessor();

}
