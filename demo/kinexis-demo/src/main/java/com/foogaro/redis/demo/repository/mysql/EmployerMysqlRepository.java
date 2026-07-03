package com.foogaro.redis.demo.repository.mysql;

import com.foogaro.redis.demo.entity.Employer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EmployerMysqlRepository extends JpaRepository<Employer, Long> {

    Employer findByEmail(String email);

}