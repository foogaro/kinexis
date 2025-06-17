package com.foogaro.redis.demo.controller;

import com.foogaro.redis.demo.entity.Employer;
import com.foogaro.redis.demo.service.EmployerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("/api/employers")
public class EmployerController {

    private final EmployerService employerService;

    public EmployerController(EmployerService employerService) {
        this.employerService = employerService;
    }

    @PostMapping
    public ResponseEntity<Void>  create(@RequestBody Employer employer) {
        employerService.save(employer);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Employer> read(@PathVariable Long id) {
        Optional<Employer> employer = employerService.findById(id);
        return employer.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PutMapping
    public ResponseEntity<Void> update(@RequestBody Employer updatedEmployer) {
        employerService.update(updatedEmployer);
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        employerService.delete(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping
    public Iterable<Employer> findAll() {
        return employerService.findAll();
    }
}
