package com.foogaro.kinexis.demo.cassandra.repository.cassandra;

import com.foogaro.kinexis.demo.cassandra.entity.Employer;
import org.springframework.data.cassandra.repository.AllowFiltering;
import org.springframework.data.cassandra.repository.CassandraRepository;

import java.util.Optional;

public interface EmployerCassandraRepository extends CassandraRepository<Employer, Long> {

    @AllowFiltering
    Optional<Employer> findByEmail(String email);
}
