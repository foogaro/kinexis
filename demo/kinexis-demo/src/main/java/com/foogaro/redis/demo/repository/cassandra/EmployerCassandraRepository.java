package com.foogaro.redis.demo.repository.cassandra;

import com.foogaro.redis.demo.entity.Employer;
import org.springframework.data.cassandra.repository.AllowFiltering;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface EmployerCassandraRepository extends CassandraRepository<Employer, Long> {

    @AllowFiltering
    Optional<Employer> findByEmail(String email);
}
