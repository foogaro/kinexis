package com.foogaro.kinexis.core.service;

import com.foogaro.kinexis.core.config.KinexisProperties;
import com.foogaro.kinexis.core.model.CachingPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KinexisStoreValidator implements SmartInitializingSingleton {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final KinexisDiagnosticsService diagnosticsService;
    private final KinexisProperties properties;

    public KinexisStoreValidator(KinexisDiagnosticsService diagnosticsService, KinexisProperties properties) {
        this.diagnosticsService = diagnosticsService;
        this.properties = properties;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (!properties.getValidation().isEnabled()) {
            logger.debug("Kinexis store validation disabled");
            return;
        }

        ValidationResult result = validate();
        result.warnings().forEach(logger::warn);
        if (!result.errors().isEmpty()) {
            String message = "Invalid Kinexis store configuration:\n - " + String.join("\n - ", result.errors());
            if (properties.getValidation().isFailFast()) {
                throw new IllegalStateException(message);
            }
            logger.error(message);
        }
    }

    public ValidationResult validate() {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        if (properties.getProcessing().getMaxParallelStores() < 1) {
            errors.add("kinexis.processing.max-parallel-stores must be greater than or equal to 1");
        }
        if (properties.getStores().getRepositoryDiscovery().isEnabled()) {
            warnings.add("kinexis.stores.repository-discovery.enabled is true; repository-name discovery is deprecated and should be used only as a migration bridge");
        }

        for (KinexisDiagnosticsService.EntityDiagnostics entity : diagnosticsService.stores()) {
            validateStoreUniqueness(entity, errors);
            if (!entity.annotated()) {
                continue;
            }
            if (!entity.enabled()) {
                if (entity.primaryStore().isEmpty()) {
                    errors.add("Entity " + entity.entityName() + " has @CachingPatterns(enabled = false) but no primary EntityStore is configured");
                }
                continue;
            }
            if (usesCache(entity) && entity.cacheStore().isEmpty()) {
                errors.add("Entity " + entity.entityName() + " uses cache patterns but no CacheStore is configured");
            }
            if (usesDatabaseRead(entity) && entity.primaryStore().isEmpty()) {
                errors.add("Entity " + entity.entityName() + " reads through a backing store but no primary EntityStore is configured");
            }
            if (entity.patterns().contains(CachingPattern.WRITE_BEHIND) && entity.targetStores().isEmpty()) {
                errors.add("Entity " + entity.entityName() + " uses WRITE_BEHIND but no target EntityStore is configured");
            }
            if (entity.patterns().contains(CachingPattern.REFRESH_AHEAD) && entity.ttl() <= 0) {
                warnings.add("Entity " + entity.entityName() + " uses REFRESH_AHEAD but ttl is not positive; expiration-based refresh will not run");
            }
        }

        return new ValidationResult(errors, warnings);
    }

    private void validateStoreUniqueness(KinexisDiagnosticsService.EntityDiagnostics entity, List<String> errors) {
        entity.stores().stream()
                .collect(Collectors.groupingBy(KinexisDiagnosticsService.StoreDiagnostics::name, Collectors.counting()))
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue() > 1)
                .map(Map.Entry::getKey)
                .forEach(name -> errors.add("Entity " + entity.entityName() + " has duplicate store name '" + name + "'"));

        entity.targetStores()
                .stream()
                .flatMap(store -> store.targets().stream().map(target -> Map.entry(target, store.name())))
                .collect(Collectors.groupingBy(Map.Entry::getKey,
                        Collectors.mapping(Map.Entry::getValue, Collectors.toSet())))
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().size() > 1)
                .forEach(entry -> errors.add("Entity " + entity.entityName()
                        + " has ambiguous target alias '" + entry.getKey()
                        + "' in stores " + entry.getValue()));
    }

    private boolean usesCache(KinexisDiagnosticsService.EntityDiagnostics entity) {
        return entity.patterns().contains(CachingPattern.CACHE_ASIDE)
                || entity.patterns().contains(CachingPattern.REFRESH_AHEAD);
    }

    private boolean usesDatabaseRead(KinexisDiagnosticsService.EntityDiagnostics entity) {
        return entity.patterns().contains(CachingPattern.CACHE_ASIDE)
                || entity.patterns().contains(CachingPattern.REFRESH_AHEAD);
    }

    public record ValidationResult(List<String> errors, List<String> warnings) {
        public ValidationResult {
            errors = List.copyOf(errors);
            warnings = List.copyOf(warnings);
        }
    }
}
