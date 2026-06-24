package com.foogaro.kinexis.demo.psql.repository.postgres;

import com.foogaro.kinexis.demo.psql.entity.Employer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface EmployerPostgresRepository extends JpaRepository<Employer, Long> {

    Optional<Employer> findByEmail(String email);
}
