package com.foogaro.redis.demo.repository.mongo;

import com.foogaro.redis.demo.entity.Employer;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EmployerMongoDbRepository extends MongoRepository<Employer, Long> {

}