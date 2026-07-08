package com.foogaro.kinexis.core.service;

import com.foogaro.kinexis.core.model.KinexisEvent;
import com.foogaro.kinexis.core.model.KinexisEventEnvelope;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class DefaultKinexisEventSchemaRegistry implements KinexisEventSchemaRegistry {

    private final boolean enabled;
    private final String defaultCurrentVersion;
    private final Map<String, String> entityVersions;
    private final List<KinexisEventUpcaster> upcasters;

    public DefaultKinexisEventSchemaRegistry(Collection<KinexisEventUpcaster> upcasters) {
        this(true, KinexisEvent.CURRENT_SCHEMA_VERSION, Map.of(), upcasters);
    }

    public DefaultKinexisEventSchemaRegistry(boolean enabled,
                                            String defaultCurrentVersion,
                                            Map<String, String> entityVersions,
                                            Collection<KinexisEventUpcaster> upcasters) {
        this.enabled = enabled;
        this.defaultCurrentVersion = normalizeVersion(defaultCurrentVersion).orElse(KinexisEvent.CURRENT_SCHEMA_VERSION);
        this.entityVersions = Map.copyOf(Objects.requireNonNullElse(entityVersions, Map.of()));
        this.upcasters = List.copyOf(Objects.requireNonNullElse(upcasters, List.of()));
    }

    @Override
    public String currentVersion(String entityType) {
        if (!enabled) {
            return KinexisEvent.CURRENT_SCHEMA_VERSION;
        }
        return Optional.ofNullable(entityVersions.get(entityType))
                .flatMap(DefaultKinexisEventSchemaRegistry::normalizeVersion)
                .orElse(defaultCurrentVersion);
    }

    @Override
    public KinexisEventEnvelope upcast(KinexisEventEnvelope envelope) {
        Objects.requireNonNull(envelope, "envelope cannot be null");
        if (!enabled) {
            return envelope;
        }
        String targetVersion = currentVersion(envelope.entityType());
        KinexisEventEnvelope current = envelope;
        while (!current.schemaVersion().equals(targetVersion)) {
            String fromVersion = current.schemaVersion();
            String entityType = current.entityType();
            Optional<KinexisEventUpcaster> matchingUpcaster = findUpcaster(entityType, fromVersion, targetVersion);
            if (matchingUpcaster.isEmpty()) {
                throw new IllegalStateException("No KinexisEventUpcaster found for entity "
                        + entityType + " from schema " + fromVersion + " to " + targetVersion);
            }
            KinexisEventUpcaster upcaster = matchingUpcaster.get();
            KinexisEventEnvelope next = Objects.requireNonNull(upcaster.upcast(current, targetVersion), "upcaster returned null");
            if (next.schemaVersion().equals(fromVersion)) {
                throw new IllegalStateException("KinexisEventUpcaster did not advance schema version for entity "
                        + current.entityType() + " from schema " + fromVersion);
            }
            current = next;
        }
        return current;
    }

    private Optional<KinexisEventUpcaster> findUpcaster(String entityType, String fromVersion, String targetVersion) {
        for (KinexisEventUpcaster upcaster : upcasters) {
            if (upcaster.supports(entityType, fromVersion, targetVersion)) {
                return Optional.of(upcaster);
            }
        }
        return Optional.empty();
    }

    private static Optional<String> normalizeVersion(String version) {
        return Optional.ofNullable(version)
                .map(String::trim)
                .filter(value -> !value.isEmpty());
    }
}
