package com.foogaro.kinexis.core.service;

import com.foogaro.kinexis.core.annotation.CachingPatterns;
import com.foogaro.kinexis.core.model.CachingPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Utility class for finding and analyzing caching pattern annotations on entity classes.
 * This class provides methods to check for various caching patterns (Cache-Aside,
 * Refresh-Ahead, Write-Behind) and caches the results to avoid repeated reflection
 * operations.
 */
@Component
public class AnnotationFinder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<Class<?>, CachingMetadata> entities = new ConcurrentHashMap<>();

    /**
     * Default constructor for AnnotationFinder.
     * Initializes the entities map for caching annotation analysis results.
     */
    public AnnotationFinder() {
    }

    /**
     * Checks if the Cache-Aside pattern is enabled for the specified entity class.
     * The result is determined by analyzing the {@link CachingPatterns} annotation on the class.
     *
     * @param entityClass the class to check for Cache-Aside pattern
     * @return true if Cache-Aside pattern is enabled, false otherwise
     */
    public boolean hasCacheAside(Class<?> entityClass) {
        return match(CachingPattern.CACHE_ASIDE.getValue(), metadata(entityClass).patterns());
    }

    /**
     * Checks if the Refresh-Ahead pattern is enabled for the specified entity class.
     * The result is determined by analyzing the {@link CachingPatterns} annotation on the class.
     *
     * @param entityClass the class to check for Refresh-Ahead pattern
     * @return true if Refresh-Ahead pattern is enabled, false otherwise
     */
    public boolean hasRefreshAhead(Class<?> entityClass) {
        return match(CachingPattern.REFRESH_AHEAD.getValue(), metadata(entityClass).patterns());
    }

    /**
     * Checks if the Write-Behind pattern is enabled for the specified entity class.
     * The result is determined by analyzing the {@link CachingPatterns} annotation on the class.
     *
     * @param entityClass the class to check for Write-Behind pattern
     * @return true if Write-Behind pattern is enabled, false otherwise
     */
    public boolean hasWriteBehind(Class<?> entityClass) {
        return match(CachingPattern.WRITE_BEHIND.getValue(), metadata(entityClass).patterns());
    }

    public boolean isEnabled(Class<?> entityClass) {
        return metadata(entityClass).enabled();
    }

    public boolean hasCachingPatterns(Class<?> entityClass) {
        return entityClass.isAnnotationPresent(CachingPatterns.class);
    }

    public Set<CachingPattern> patterns(Class<?> entityClass) {
        if (!entityClass.isAnnotationPresent(CachingPatterns.class)) {
            return Set.of(CachingPattern.NONE);
        }
        return Arrays.stream(entityClass.getAnnotation(CachingPatterns.class).patterns())
                .collect(Collectors.toUnmodifiableSet());
    }

    public long ttl(Class<?> entityClass) {
        return metadata(entityClass).ttl();
    }

    /**
     * Analyzes the caching patterns for an entity class and caches the result.
     * If the class has not been analyzed before, it checks for the {@link CachingPatterns} annotation
     * and combines the values of all specified patterns. The result is stored in the entities map
     * for future lookups.
     *
     * @param entityClass the class to analyze for caching patterns
     */
    private CachingMetadata metadata(Class<?> entityClass) {
        return entities.computeIfAbsent(entityClass, ignored -> {
            int cacheType = CachingPattern.NONE.getValue();
            boolean enabled = true;
            long ttl = 0;
            if (entityClass.isAnnotationPresent(CachingPatterns.class)) {
                CachingPatterns cachingPatterns = entityClass.getAnnotation(CachingPatterns.class);
                enabled = cachingPatterns.enabled();
                ttl = cachingPatterns.ttl();
                for (CachingPattern pattern : cachingPatterns.patterns()) {
                    cacheType = cacheType + pattern.getValue();
                }
            }
            logger.debug("Resolved Kinexis metadata for {}: enabled={}, ttl={}, patterns={}",
                    entityClass.getSimpleName(), enabled, ttl, cacheType);
            return new CachingMetadata(cacheType, enabled, ttl);
        });
    }

    /**
     * Checks if a specific caching pattern is enabled in the entity's cache type.
     * Uses bitwise operations to check if the pattern is present in the combined cache type value.
     *
     * @param cacheType the caching pattern value to check for
     * @param entityCacheType the combined cache type value of the entity
     * @return true if the pattern is enabled, false otherwise
     */
    private boolean match(int cacheType, int entityCacheType) {
        return (cacheType & entityCacheType) > 0;
    }

    private record CachingMetadata(int patterns, boolean enabled, long ttl) {
    }
}
