package com.foogaro.kinexis.demo.sqlserver.repository.redis;

import com.foogaro.kinexis.demo.sqlserver.entity.Employer;
import com.redis.om.spring.repository.RedisDocumentRepository;

public interface EmployerRedisRepository extends RedisDocumentRepository<Employer, Long> {
}
