package com.foogaro.kinexis.core.store;

import com.foogaro.kinexis.core.service.BeanFinder;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.Repository;

import java.util.List;
import java.util.Optional;

@Deprecated(forRemoval = false)
public class BeanFinderEntityStoreRegistry implements EntityStoreRegistry {

    private static final String CACHE_REPOSITORY_SUFFIX = "RedisRepository";
    private static final String PRIMARY_REPOSITORY_SUFFIX = "Repository";

    private final BeanFinder beanFinder;

    public BeanFinderEntityStoreRegistry(BeanFinder beanFinder) {
        this.beanFinder = beanFinder;
    }

    public BeanFinderEntityStoreRegistry(BeanFinder beanFinder, Object ignoredRedisTemplate) {
        this(beanFinder);
    }

    @Override
    public <T> Optional<CacheStore<T>> findCacheStore(Class<T> entityType) {
        String repositoryName = entityType.getSimpleName() + CACHE_REPOSITORY_SUFFIX;
        return beanFinder.<T>findRepositoriesForEntity(repositoryName)
                .stream()
                .filter(repository -> repository instanceof CrudRepository)
                .findFirst()
                .map(repository -> new CrudRepositoryCacheStore<>(repositoryName, entityType, (CrudRepository<T, ?>) repository, beanFinder, java.util.Set.of(repositoryName)));
    }

    @Override
    public <T> Optional<EntityStore<T>> findPrimaryStore(Class<T> entityType) {
        String repositoryName = entityType.getSimpleName() + PRIMARY_REPOSITORY_SUFFIX;
        return beanFinder.<T>findRepositoriesForEntity(repositoryName)
                .stream()
                .filter(repository -> repository instanceof CrudRepository)
                .findFirst()
                .map(repository -> new CrudRepositoryEntityStore<>(repositoryName, entityType, (CrudRepository<T, ?>) repository, beanFinder));
    }

    @Override
    public <T> List<EntityStore<T>> findTargetStores(Class<T> entityType) {
        return beanFinder.findRepositoriesForEntity(entityType)
                .stream()
                .filter(repository -> repository instanceof CrudRepository)
                .filter(repository -> !storeName(repository).endsWith(CACHE_REPOSITORY_SUFFIX))
                .map(repository -> (EntityStore<T>) new CrudRepositoryEntityStore<>(storeName(repository), entityType, (CrudRepository<T, ?>) repository, beanFinder))
                .toList();
    }

    @Override
    public <T> List<EntityStore<T>> findTargetStores(Class<T> entityType, java.util.Collection<String> targets) {
        if (targets == null || targets.isEmpty()) {
            return findTargetStores(entityType);
        }
        return findTargetStores(entityType)
                .stream()
                .filter(store -> store.targets().stream().anyMatch(targets::contains))
                .toList();
    }

    private String storeName(Repository<?, ?> repository) {
        return java.util.Arrays.stream(repository.getClass().getInterfaces())
                .filter(Repository.class::isAssignableFrom)
                .map(Class::getSimpleName)
                .findFirst()
                .orElse(repository.getClass().getSimpleName());
    }
}
