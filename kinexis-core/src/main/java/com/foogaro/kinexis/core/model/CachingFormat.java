package com.foogaro.kinexis.core.model;

/**
 * Enum representing the different formats for caching data in Redis.
 * This enum defines the supported serialization formats for storing entity data in Redis.
 * 
 * <p>The available formats are:</p>
 * <ul>
 *     <li>{@link #JSON} - Stores data as JSON strings, providing a human-readable format
 *         that is easily debuggable and compatible with various tools.</li>
 *     <li>{@link #HASH} - Stores data as Redis hashes, providing a more efficient storage
 *         format that is optimized for Redis operations and memory usage.</li>
 * </ul>
 */
public enum CachingFormat {

    /**
     * JSON format for caching data.
     * This format serializes entities to JSON strings, which are then stored in Redis.
     * Benefits:
     * - Human-readable format
     * - Easy to debug and inspect
     * - Compatible with various tools and clients
     * - Flexible schema evolution
     */
    JSON,

    /**
     * Hash format for caching data.
     * This format stores entity fields as Redis hash fields, providing a more efficient
     * storage mechanism.
     * Benefits:
     * - More efficient memory usage
     * - Better performance for partial updates
     * - Native Redis data structure
     * - Optimized for Redis operations
     * - TTL on single field
     */
    HASH

}
