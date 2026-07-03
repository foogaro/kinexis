package com.foogaro.kinexis.demo.sqlserver.repository.sqlserver;

import com.foogaro.kinexis.demo.sqlserver.entity.Employer;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface EmployerSqlServerRepository extends JpaRepository<Employer, Long> {

    Optional<Employer> findByEmail(String email);
}
