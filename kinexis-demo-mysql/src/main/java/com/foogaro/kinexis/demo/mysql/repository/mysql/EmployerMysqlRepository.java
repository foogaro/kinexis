package com.foogaro.kinexis.demo.mysql.repository.mysql;

import com.foogaro.kinexis.demo.mysql.entity.Employer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface EmployerMysqlRepository extends JpaRepository<Employer, Long> {

    Optional<Employer> findByEmail(String email);
}
