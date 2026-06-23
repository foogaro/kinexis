package com.foogaro.kinexis.core.store;

import com.foogaro.kinexis.core.service.BeanFinder;
import org.springframework.data.repository.CrudRepository;

import java.util.Collection;
import java.util.Optional;
import java.util.LinkedHashSet;
import java.util.Set;

public class CrudRepositoryCacheStore<T> implements CacheStore<T> {

    private final CrudRepositoryEntityStore<T> delegate;

    public CrudRepositoryCacheStore(String name, Class<T> entityType, CrudRepository<T, ?> repository, BeanFinder beanFinder) {
        this(name, entityType, repository, beanFinder, Set.of(name));
    }

    public CrudRepositoryCacheStore(String name, Class<T> entityType, CrudRepository<T, ?> repository, BeanFinder beanFinder, Set<String> targets) {
        this.delegate = new CrudRepositoryEntityStore<>(name, entityType, repository, beanFinder, targets);
    }

    public static <T> Builder<T> builder(Class<T> entityType, CrudRepository<T, ?> repository, BeanFinder beanFinder) {
        return new Builder<>(entityType, repository, beanFinder);
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

        public CrudRepositoryCacheStore<T> build() {
            String storeName = name == null ? entityType.getSimpleName() + "CacheStore" : name;
            return new CrudRepositoryCacheStore<>(storeName, entityType, repository, beanFinder, targets);
        }
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public Class<T> entityType() {
        return delegate.entityType();
    }

    @Override
    public Set<String> targets() {
        return delegate.targets();
    }

    @Override
    public Optional<T> findById(Object id) {
        return delegate.findById(id);
    }

    @Override
    public T save(T entity) {
        return delegate.save(entity);
    }

    @Override
    public void deleteById(Object id) {
        delegate.deleteById(id);
    }
}
