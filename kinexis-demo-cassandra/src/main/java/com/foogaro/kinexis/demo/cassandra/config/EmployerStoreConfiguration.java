package com.foogaro.kinexis.demo.cassandra.config;

import com.foogaro.kinexis.core.service.BeanFinder;
import com.foogaro.kinexis.core.store.CacheStore;
import com.foogaro.kinexis.core.store.CrudRepositoryEntityStore;
import com.foogaro.kinexis.core.store.EntityStore;
import com.foogaro.kinexis.core.store.RedisOmCacheStore;
import com.foogaro.kinexis.demo.cassandra.entity.Employer;
import com.foogaro.kinexis.demo.cassandra.repository.cassandra.EmployerCassandraRepository;
import com.foogaro.kinexis.demo.cassandra.repository.redis.EmployerRedisRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

@Configuration
public class EmployerStoreConfiguration {

    @Bean
    public EntityStore<Employer> employerCassandraStore(EmployerCassandraRepository repository, BeanFinder beanFinder) {
        return CrudRepositoryEntityStore.builder(Employer.class, repository, beanFinder)
                .name("employerCassandraStore")
                .targets("cassandra", "primary")
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
