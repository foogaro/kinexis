package com.foogaro.kinexis.core.store;

import com.foogaro.kinexis.core.Misc;
import com.foogaro.kinexis.core.service.BeanFinder;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.LinkedHashSet;
import java.util.Set;

public class RedisOmCacheStore<T> implements CacheStore<T> {

    private final CrudRepositoryCacheStore<T> delegate;
    private final RedisTemplate<String, String> redisTemplate;

    public RedisOmCacheStore(String name, Class<T> entityType, CrudRepository<T, ?> repository, BeanFinder beanFinder) {
        this(name, entityType, repository, beanFinder, Set.of(name), null);
    }

    public RedisOmCacheStore(String name, Class<T> entityType, CrudRepository<T, ?> repository, BeanFinder beanFinder, Set<String> targets) {
        this(name, entityType, repository, beanFinder, targets, null);
    }

    public RedisOmCacheStore(String name, Class<T> entityType, CrudRepository<T, ?> repository, BeanFinder beanFinder,
                             Set<String> targets, RedisTemplate<String, String> redisTemplate) {
        this.delegate = new CrudRepositoryCacheStore<>(name, entityType, repository, beanFinder, targets);
        this.redisTemplate = redisTemplate;
    }

    public static <T> Builder<T> builder(Class<T> entityType, CrudRepository<T, ?> repository, BeanFinder beanFinder) {
        return new Builder<>(entityType, repository, beanFinder);
    }

    public static class Builder<T> {

        private final Class<T> entityType;
        private final CrudRepository<T, ?> repository;
        private final BeanFinder beanFinder;
        private String name;
        private RedisTemplate<String, String> redisTemplate;
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

        public Builder<T> redisTemplate(RedisTemplate<String, String> redisTemplate) {
            this.redisTemplate = redisTemplate;
            return this;
        }

        public RedisOmCacheStore<T> build() {
            String storeName = name == null ? entityType.getSimpleName() + "RedisOmStore" : name;
            return new RedisOmCacheStore<>(storeName, entityType, repository, beanFinder, targets, redisTemplate);
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
    public T save(T entity, Duration ttl) {
        T saved = save(entity);
        if (redisTemplate != null && ttl != null && !ttl.isZero() && !ttl.isNegative()) {
            Misc.getEntityKey(saved).ifPresent(key -> redisTemplate.expire(key, ttl));
        }
        return saved;
    }

    @Override
    public void deleteById(Object id) {
        delegate.deleteById(id);
    }
}
