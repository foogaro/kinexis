package com.foogaro.kinexis.demo.cassandra.service;

import com.foogaro.kinexis.core.service.KinexisService;
import com.foogaro.kinexis.demo.cassandra.entity.Employer;
import com.foogaro.kinexis.demo.cassandra.repository.cassandra.EmployerCassandraRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class EmployerService extends KinexisService<Employer> {

    private final EmployerCassandraRepository repository;

    public EmployerService(EmployerCassandraRepository repository) {
        this.repository = repository;
    }

    public void create(Employer employer) {
        save(employer);
    }

    public Optional<Employer> read(Long id) {
        return findById(id);
    }

    public void update(Long id, Employer employer) {
        employer.setId(id);
        invalidateCache(id);
        super.update(employer);
    }

    public void remove(Long id) {
        invalidateCache(id);
        delete(id);
    }

    public List<Employer> findAll() {
        return repository.findAll();
    }

    public Optional<Employer> findByEmail(String email) {
        return repository.findByEmail(email);
    }
}
