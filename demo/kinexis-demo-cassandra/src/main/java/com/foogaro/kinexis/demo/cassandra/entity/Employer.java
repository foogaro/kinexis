package com.foogaro.kinexis.demo.cassandra.entity;

import com.foogaro.kinexis.core.annotation.CachingPatterns;
import com.foogaro.kinexis.core.model.CachingFormat;
import com.foogaro.kinexis.core.model.CachingPattern;
import com.redis.om.spring.annotations.Document;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.annotation.Id;

@Table("employers")
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
    @PrimaryKey
    private Long id;

    @Column("name")
    private String name;

    @Column("address")
    private String address;

    @Column("email")
    private String email;

    @Column("phone")
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
