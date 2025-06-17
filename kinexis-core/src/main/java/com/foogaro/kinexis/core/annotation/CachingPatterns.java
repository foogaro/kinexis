package com.foogaro.kinexis.core.annotation;

import com.foogaro.kinexis.core.model.CachingFormat;
import com.foogaro.kinexis.core.model.CachingPattern;
import com.foogaro.kinexis.core.processor.CachingPatternsAnnotationProcessor;

import java.lang.annotation.*;

/**
 * Annotation to specify caching patterns for an entity.
 * This annotation is used to define how an entity should be cached in Redis,
 * supporting multiple caching patterns that can be combined:
 * <ul>
 *     <li>{@link CachingPattern#CACHE_ASIDE}: Cache data alongside the database</li>
 *     <li>{@link CachingPattern#REFRESH_AHEAD}: Proactively refresh cache before expiration</li>
 *     <li>{@link CachingPattern#WRITE_BEHIND}: Write to cache first, then asynchronously to database</li>
 * </ul>
 * The annotation processor will automatically generate the necessary components
 * to implement the specified caching patterns.
 *
 * @see CachingPattern
 * @see CachingPatternsAnnotationProcessor
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CachingPatterns {
    /**
     * Specifies the caching patterns to be used for the annotated entity.
     * Multiple patterns can be combined using bitwise OR (|).
     * For example: @CachingPatterns(patterns = {CachingPattern.CACHE_ASIDE, CachingPattern.WRITE_BEHIND})
     *
     * @return array of caching patterns
     */
    CachingPattern[] patterns() default {CachingPattern.NONE};

    /**
     * Specifies in which format, HASH or JSON document, the entity will be stored into the Cache.
     * @return the caching format, which is {@link CachingFormat#JSON} by default.
     */
    CachingFormat format() default CachingFormat.JSON;

    /**
     * Specifies whether the caching patterns should be enabled by default.
     * If false, the patterns will need to be explicitly enabled at runtime.
     *
     * @return true if patterns are enabled by default, false otherwise
     */
    boolean enabled() default true;

    /**
     * Specifies the TTL (Time To Live) in seconds for cached entries.
     * A value of 0 or negative means no expiration.
     *
     * @return TTL in seconds
     */
    long ttl() default 0;
}
