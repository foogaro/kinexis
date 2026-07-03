package com.foogaro.kinexis.demo.mongodb.entity;

import com.foogaro.kinexis.core.annotation.CachingPatterns;
import com.foogaro.kinexis.core.model.CachingFormat;
import com.foogaro.kinexis.core.model.CachingPattern;
import com.redis.om.spring.annotations.Document;
import org.springframework.data.annotation.Id;

@org.springframework.data.mongodb.core.mapping.Document(collection = "employers")
@Document("employers")
@CachingPatterns(
        format = CachingFormat.JSON,
        patterns = {
                CachingPattern.WRITE_BEHIND,
                CachingPattern.CACHE_ASIDE,
                CachingPattern.REFRESH_AHEAD
        },
        ttl = 300
)
public class Employer {

    @jakarta.persistence.Id
    @Id
    private Long id;

    private String name;

    private String address;

    private String email;

    private String phone;

    public Employer() {
    }

    public Employer(Long id, String name, String address, String email, String phone) {
        this.id = id;
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
}
