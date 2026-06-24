package com.foogaro.kinexis.demo.mysql.repository.redis;

import com.foogaro.kinexis.demo.mysql.entity.Employer;
import com.redis.om.spring.repository.RedisDocumentRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EmployerRedisRepository extends RedisDocumentRepository<Employer, Long> {
}
