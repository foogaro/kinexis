package com.foogaro.redis.demo.repository.mongodb;

import com.foogaro.redis.demo.entity.Employer;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface EmployerMongodbRepository extends MongoRepository<Employer, Long> {

    Optional<Employer> findByEmail(String email);
}
