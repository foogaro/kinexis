package com.foogaro.redis.demo.entity;

import com.foogaro.kinexis.core.annotation.CachingPatterns;
import com.foogaro.kinexis.core.model.CachingFormat;
import com.foogaro.kinexis.core.model.CachingPattern;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.springframework.data.mongodb.core.mapping.Document;

@Entity
@Table(name = "employers")
@Document(collection = "employers")
@CachingPatterns(format = CachingFormat.JSON, patterns = {CachingPattern.WRITE_BEHIND, CachingPattern.CACHE_ASIDE, CachingPattern.REFRESH_AHEAD})
public class Employer {

    @Id
    private Long id;

    @Column(name = "name", nullable = false)
    private String name;

    @Column(name = "address", nullable = false)
    private String address;

    @Column(name = "email", unique = true, nullable = false)
    private String email;

    @Column(name = "phone", nullable = true)
    private String phone;

    public Employer() {
    }

    public Employer(String name, String address, String email, String phone) {
        this.name = name;
        this.address = address;
        this.email = email;
        this.phone = phone;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Employer{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", address='").append(address).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", phone='").append(phone).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
