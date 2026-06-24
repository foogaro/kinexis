package com.foogaro.kinexis.demo.cassandra.repository.redis;

import com.foogaro.kinexis.demo.cassandra.entity.Employer;
import com.redis.om.spring.repository.RedisDocumentRepository;

public interface EmployerRedisRepository extends RedisDocumentRepository<Employer, Long> {
}
