package com.foogaro.kinexis.core.orchestrator;

import com.foogaro.kinexis.core.exception.AcknowledgeMessageException;
import com.foogaro.kinexis.core.exception.ProcessMessageException;
import com.foogaro.kinexis.core.processor.Processor;
import org.springframework.data.redis.connection.stream.MapRecord;

/**
 * Abstract base class for process orchestrators that handle Redis Stream records.
 * This class provides a default implementation of the orchestration process,
 * including message processing and acknowledgment phases.
 *
 * @param <T> the type of entity being processed
 * @param <R> the type of repository used for entity operations
 */
public abstract class AbstractProcessOrchestrator<T, R> implements ProcessOrchestrator<T, R> {

    /**
     * Default constructor for AbstractProcessOrchestrator.
     * This constructor is used by Spring to create instances of this class.
     */
    protected AbstractProcessOrchestrator() {
    }

    /**
     * Orchestrates the processing of a Redis Stream record.
     * This implementation:
     * 1. Processes the record using the provided processor
     * 2. Acknowledges the record if processing is successful
     * 3. Handles exceptions by wrapping them in RuntimeException
     *
     * @param record the Redis Stream record to process
     * @param processor the processor to use for handling the record
     * @throws RuntimeException if either processing or acknowledgment fails
     */
    @Override
    public void orchestrate(MapRecord<String, String, String> record, Processor<T, R> processor) {
        try {
            processor.process(record);
            processor.acknowledge(record);
        } catch (ProcessMessageException e) {
            throw new RuntimeException(e);
        } catch (AcknowledgeMessageException e) {
            throw new RuntimeException(e);
        }
    }

}
