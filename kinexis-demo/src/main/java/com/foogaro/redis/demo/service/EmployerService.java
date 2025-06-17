package com.foogaro.redis.demo.service;

import com.foogaro.kinexis.core.service.KinexisService;
import com.foogaro.redis.demo.entity.Employer;
import com.foogaro.redis.demo.repository.EmployerRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EmployerService extends KinexisService<Employer> {

    private final EmployerRepository repository;

    public EmployerService(EmployerRepository repository) {
        this.repository = repository;
    }

    public List<Employer> findAll() {
        return repository.findAll();
    }

    public Employer getEmployerByEmail(String email) {
        return repository.findByEmail(email);
    }
}
