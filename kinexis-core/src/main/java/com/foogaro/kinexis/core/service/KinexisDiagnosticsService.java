package com.foogaro.kinexis.core.service;

import com.foogaro.kinexis.core.model.CachingPattern;
import com.foogaro.kinexis.core.processor.Processor;
import com.foogaro.kinexis.core.store.CacheStore;
import com.foogaro.kinexis.core.store.EntityStore;
import com.foogaro.kinexis.core.store.EntityStoreRegistry;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class KinexisDiagnosticsService {

    private final List<EntityStore<?>> explicitStores;
    private final List<Processor<?>> processors;
    private final List<KinexisService<?>> services;
    private final List<KinexisEntityRegistry> entityRegistries;
    private final EntityStoreRegistry entityStoreRegistry;
    private final AnnotationFinder annotationFinder;

    public KinexisDiagnosticsService(Collection<EntityStore<?>> explicitStores,
                                     Collection<Processor<?>> processors,
                                     Collection<KinexisService<?>> services,
                                     Collection<KinexisEntityRegistry> entityRegistries,
                                     EntityStoreRegistry entityStoreRegistry,
                                     AnnotationFinder annotationFinder) {
        this.explicitStores = List.copyOf(explicitStores);
        this.processors = List.copyOf(processors);
        this.services = List.copyOf(services);
        this.entityRegistries = List.copyOf(entityRegistries);
        this.entityStoreRegistry = entityStoreRegistry;
        this.annotationFinder = annotationFinder;
    }

    public List<EntityDiagnostics> stores() {
        return entityTypes()
                .map(this::entity)
                .sorted(Comparator.comparing(EntityDiagnostics::entityName))
                .toList();
    }

    public EntityDiagnostics entity(Class<?> entityType) {
        Optional<? extends CacheStore<?>> cacheStore = entityStoreRegistry.findCacheStore(entityType);
        Optional<? extends EntityStore<?>> primaryStore = entityStoreRegistry.findPrimaryStore(entityType);
        List<? extends EntityStore<?>> targetStores = entityStoreRegistry.findTargetStores(entityType);
        List<StoreDiagnostics> stores = knownStores(entityType, cacheStore, primaryStore, targetStores);
        return new EntityDiagnostics(
                entityType,
                entityType.getName(),
                annotationFinder.hasCachingPatterns(entityType),
                annotationFinder.isEnabled(entityType),
                annotationFinder.patterns(entityType),
                annotationFinder.ttl(entityType),
                cacheStore.map(this::store),
                primaryStore.map(this::store),
                targetStores.stream().map(this::store).toList(),
                stores);
    }

    private Stream<Class<?>> entityTypes() {
        return Stream.concat(
                        explicitStores.stream().map(EntityStore::entityType),
                        Stream.concat(
                                processors.stream().map(Processor::getEntityClass),
                                Stream.concat(
                                        services.stream().map(KinexisService::getEntityClass),
                                        entityRegistries.stream().flatMap(registry -> registry.entityTypes().stream()))))
                .distinct();
    }

    private List<StoreDiagnostics> knownStores(Class<?> entityType,
                                               Optional<? extends CacheStore<?>> cacheStore,
                                               Optional<? extends EntityStore<?>> primaryStore,
                                               List<? extends EntityStore<?>> targetStores) {
        return Stream.concat(
                        explicitStores.stream().filter(store -> store.entityType().equals(entityType)),
                        Stream.concat(
                                cacheStore.stream(),
                                Stream.concat(primaryStore.stream(), targetStores.stream())))
                .map(this::store)
                .distinct()
                .toList();
    }

    private StoreDiagnostics store(EntityStore<?> store) {
        return new StoreDiagnostics(
                store.name(),
                store.entityType(),
                store.entityType().getName(),
                store.targets(),
                store instanceof CacheStore<?>);
    }

    public record EntityDiagnostics(Class<?> entityType,
                                    String entityName,
                                    boolean annotated,
                                    boolean enabled,
                                    Set<CachingPattern> patterns,
                                    long ttl,
                                    Optional<StoreDiagnostics> cacheStore,
                                    Optional<StoreDiagnostics> primaryStore,
                                    List<StoreDiagnostics> targetStores,
                                    List<StoreDiagnostics> stores) {
    }

    public record StoreDiagnostics(String name,
                                   Class<?> entityType,
                                   String entityName,
                                   Set<String> targets,
                                   boolean cache) {
    }
}
