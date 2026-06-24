package com.foogaro.kinexis.demo.sqlserver.config;

import com.foogaro.kinexis.core.service.BeanFinder;
import com.foogaro.kinexis.core.store.CacheStore;
import com.foogaro.kinexis.core.store.CrudRepositoryEntityStore;
import com.foogaro.kinexis.core.store.EntityStore;
import com.foogaro.kinexis.core.store.RedisOmCacheStore;
import com.foogaro.kinexis.demo.sqlserver.entity.Employer;
import com.foogaro.kinexis.demo.sqlserver.repository.redis.EmployerRedisRepository;
import com.foogaro.kinexis.demo.sqlserver.repository.sqlserver.EmployerSqlServerRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

@Configuration
public class EmployerStoreConfiguration {

    @Bean
    public EntityStore<Employer> employerSqlServerStore(EmployerSqlServerRepository repository, BeanFinder beanFinder) {
        return CrudRepositoryEntityStore.builder(Employer.class, repository, beanFinder)
                .name("employerSqlServerStore")
                .targets("sqlserver", "primary")
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
