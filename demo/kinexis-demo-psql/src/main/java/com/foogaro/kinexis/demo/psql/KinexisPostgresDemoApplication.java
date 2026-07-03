package com.foogaro.kinexis.demo.psql;

import com.foogaro.kinexis.core.config.KinexisConfiguration;
import com.redis.om.spring.annotations.EnableRedisDocumentRepositories;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableJpaRepositories(basePackages = "com.foogaro.kinexis.demo.psql.repository.postgres")
@EnableRedisDocumentRepositories(basePackages = "com.foogaro.kinexis.demo.psql.repository.redis")
@EnableScheduling
@Import(KinexisConfiguration.class)
public class KinexisPostgresDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KinexisPostgresDemoApplication.class, args);
    }
}
