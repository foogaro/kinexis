package com.foogaro.kinexis.demo.psql.config;

import com.foogaro.kinexis.core.service.BeanFinder;
import com.foogaro.kinexis.core.store.CacheStore;
import com.foogaro.kinexis.core.store.CrudRepositoryEntityStore;
import com.foogaro.kinexis.core.store.EntityStore;
import com.foogaro.kinexis.core.store.RedisOmCacheStore;
import com.foogaro.kinexis.demo.psql.entity.Employer;
import com.foogaro.kinexis.demo.psql.repository.postgres.EmployerPostgresRepository;
import com.foogaro.kinexis.demo.psql.repository.redis.EmployerRedisRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;

@Configuration
public class EmployerStoreConfiguration {

    @Bean
    public EntityStore<Employer> employerPostgresStore(EmployerPostgresRepository repository, BeanFinder beanFinder) {
        return CrudRepositoryEntityStore.builder(Employer.class, repository, beanFinder)
                .name("employerPostgresStore")
                .targets("postgresql", "primary")
                .build();
    }

    @Bean
    public CacheStore<Employer> employerRedisCacheStore(EmployerRedisRepository repository,
                                                        BeanFinder beanFinder,
                                                        @Qualifier("redisTemplate") RedisTemplate<String, String> redisTemplate) {
        return RedisOmCacheStore.builder(Employer.class, repository, beanFinder)
                .name("employerRedisCacheStore")
                .targets("redis", "cache")
                .redisTemplate(redisTemplate)
                .build();
    }
}
