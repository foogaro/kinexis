package com.foogaro.kinexis.demo.mongodb.controller;

import com.foogaro.kinexis.demo.mongodb.entity.Employer;
import com.foogaro.kinexis.demo.mongodb.service.EmployerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/api/employers")
public class EmployerController {

    private final EmployerService employerService;

    public EmployerController(EmployerService employerService) {
        this.employerService = employerService;
    }

    @PostMapping
    public ResponseEntity<Void> create(@RequestBody Employer employer) {
        employerService.create(employer);
        return ResponseEntity.created(URI.create("/api/employers/" + employer.getId())).build();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Employer> read(@PathVariable Long id) {
        return employerService.read(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public ResponseEntity<Void> update(@PathVariable Long id, @RequestBody Employer employer) {
        employerService.update(id, employer);
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        employerService.remove(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping
    public ResponseEntity<List<Employer>> findAll() {
        return ResponseEntity.ok(employerService.findAll());
    }

    @GetMapping("/email/{email}")
    public ResponseEntity<Employer> findByEmail(@PathVariable String email) {
        return employerService.findByEmail(email)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
