package com.foogaro.redis.demo.repository.sqlserver;

import com.foogaro.redis.demo.entity.Employer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface EmployerSqlServerRepository extends JpaRepository<Employer, Long> {

    Optional<Employer> findByEmail(String email);
}
