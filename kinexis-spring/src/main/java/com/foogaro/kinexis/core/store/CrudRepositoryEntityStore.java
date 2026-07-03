package com.foogaro.kinexis.core.store;

import com.foogaro.kinexis.core.service.BeanFinder;
import org.springframework.data.repository.CrudRepository;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class CrudRepositoryEntityStore<T> implements EntityStore<T> {

    private final String name;
    private final Class<T> entityType;
    private final CrudRepository<T, ?> repository;
    private final BeanFinder beanFinder;
    private final Set<String> targets;

    public CrudRepositoryEntityStore(String name, Class<T> entityType, CrudRepository<T, ?> repository, BeanFinder beanFinder) {
        this(name, entityType, repository, beanFinder, Set.of(name));
    }

    public CrudRepositoryEntityStore(String name, Class<T> entityType, CrudRepository<T, ?> repository, BeanFinder beanFinder, Set<String> targets) {
        this.name = requireText(name, "name");
        this.entityType = Objects.requireNonNull(entityType, "entityType cannot be null");
        this.repository = Objects.requireNonNull(repository, "repository cannot be null");
        this.beanFinder = Objects.requireNonNull(beanFinder, "beanFinder cannot be null");
        this.targets = normalizeTargets(targets, this.name);
    }

    public static <T> Builder<T> builder(Class<T> entityType, CrudRepository<T, ?> repository, BeanFinder beanFinder) {
        return new Builder<>(entityType, repository, beanFinder);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Class<T> entityType() {
        return entityType;
    }

    @Override
    public Set<String> targets() {
        return targets;
    }

    @Override
    public Optional<T> findById(Object id) {
        CrudRepository<T, Object> crudRepository = beanFinder.asCrudRepository(repository);
        Class<?> idType = beanFinder.getIdType(repository);
        Object storeId = id;
        if (idType != null && !idType.isInstance(id)) {
            storeId = beanFinder.createId(idType, String.valueOf(id));
        }
        return crudRepository.findById(storeId);
    }

    @Override
    public T save(T entity) {
        CrudRepository<T, Object> crudRepository = beanFinder.asCrudRepository(repository);
        return crudRepository.save(entity);
    }

    @Override
    public void deleteById(Object id) {
        beanFinder.executeIdOperation(repository, String.valueOf(id), CrudRepository::deleteById);
    }

    static Set<String> normalizeTargets(Collection<String> targets, String defaultTarget) {
        LinkedHashSet<String> normalized = new LinkedHashSet<>();
        if (targets != null) {
            targets.stream()
                    .filter(Objects::nonNull)
                    .map(String::trim)
                    .filter(target -> !target.isEmpty())
                    .forEach(normalized::add);
        }
        if (normalized.isEmpty()) {
            normalized.add(requireText(defaultTarget, "defaultTarget"));
        }
        return Set.copyOf(normalized);
    }

    static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " cannot be null or blank");
        }
        return value.trim();
    }

    public static class Builder<T> {

        private final Class<T> entityType;
        private final CrudRepository<T, ?> repository;
        private final BeanFinder beanFinder;
        private String name;
        private final Set<String> targets = new LinkedHashSet<>();

        private Builder(Class<T> entityType, CrudRepository<T, ?> repository, BeanFinder beanFinder) {
            this.entityType = entityType;
            this.repository = repository;
            this.beanFinder = beanFinder;
        }

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> target(String target) {
            this.targets.add(target);
            return this;
        }

        public Builder<T> targets(String... targets) {
            if (targets != null) {
                for (String target : targets) {
                    this.targets.add(target);
                }
            }
            return this;
        }

        public Builder<T> targets(Collection<String> targets) {
            if (targets != null) {
                this.targets.addAll(targets);
            }
            return this;
        }

        public CrudRepositoryEntityStore<T> build() {
            String storeName = name == null ? entityType.getSimpleName() + "Store" : name;
            return new CrudRepositoryEntityStore<>(storeName, entityType, repository, beanFinder, targets);
        }
    }
}
