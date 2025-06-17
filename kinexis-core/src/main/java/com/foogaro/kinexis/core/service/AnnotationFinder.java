package com.foogaro.kinexis.core.service;

import com.foogaro.kinexis.core.annotation.CachingPatterns;
import com.foogaro.kinexis.core.model.CachingPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for finding and analyzing caching pattern annotations on entity classes.
 * This class provides methods to check for various caching patterns (Cache-Aside,
 * Refresh-Ahead, Write-Behind) and caches the results to avoid repeated reflection
 * operations.
 */
@Component
public class AnnotationFinder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<Class<?>, Integer> entities = new HashMap<>();

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
        checkMap(entityClass);
        return match(CachingPattern.CACHE_ASIDE.getValue(), entities.get(entityClass));
    }

    /**
     * Checks if the Refresh-Ahead pattern is enabled for the specified entity class.
     * The result is determined by analyzing the {@link CachingPatterns} annotation on the class.
     *
     * @param entityClass the class to check for Refresh-Ahead pattern
     * @return true if Refresh-Ahead pattern is enabled, false otherwise
     */
    public boolean hasRefreshAhead(Class<?> entityClass) {
        checkMap(entityClass);
        return match(CachingPattern.REFRESH_AHEAD.getValue(), entities.get(entityClass));
    }

    /**
     * Checks if the Write-Behind pattern is enabled for the specified entity class.
     * The result is determined by analyzing the {@link CachingPatterns} annotation on the class.
     *
     * @param entityClass the class to check for Write-Behind pattern
     * @return true if Write-Behind pattern is enabled, false otherwise
     */
    public boolean hasWriteBehind(Class<?> entityClass) {
        checkMap(entityClass);
        return match(CachingPattern.WRITE_BEHIND.getValue(), entities.get(entityClass));
    }

    /**
     * Analyzes the caching patterns for an entity class and caches the result.
     * If the class has not been analyzed before, it checks for the {@link CachingPatterns} annotation
     * and combines the values of all specified patterns. The result is stored in the entities map
     * for future lookups.
     *
     * @param entityClass the class to analyze for caching patterns
     */
    private void checkMap(Class<?> entityClass) {
        Integer cacheType = entities.get(entityClass);
        if (cacheType == null) {
            cacheType = CachingPattern.NONE.getValue();
            if (entityClass.isAnnotationPresent(CachingPatterns.class)) {
                CachingPatterns cachingPatterns = entityClass.getAnnotation(CachingPatterns.class);
                for (CachingPattern pattern : cachingPatterns.patterns()) {
                    cacheType = cacheType + pattern.getValue();
                }
            }
            entities.put(entityClass, cacheType);
        }
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
}
