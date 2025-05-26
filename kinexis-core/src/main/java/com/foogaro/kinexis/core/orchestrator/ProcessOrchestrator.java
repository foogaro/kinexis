package com.foogaro.kinexis.core.orchestrator;

import com.foogaro.kinexis.core.processor.Processor;
import org.springframework.data.redis.connection.stream.MapRecord;

/**
 * Interface for orchestrating the processing of Redis Stream records.
 * This interface defines the contract for classes that coordinate the processing
 * of messages in a Redis Stream, managing the flow between record processing
 * and acknowledgment.
 *
 * @param <T> the type of entity that this orchestrator handles
 * @param <R> the type of repository used for entity operations
 */
public interface ProcessOrchestrator<T, R> {

    /**
     * Orchestrates the processing of a Redis Stream record.
     * This method coordinates the execution of the processor on the given record,
     * handling any necessary pre-processing, post-processing, or error handling.
     *
     * @param record the Redis Stream record to process
     * @param processor the processor to use for handling the record
     */
    void orchestrate(MapRecord<String, String, String> record, Processor<T, R> processor);

}
