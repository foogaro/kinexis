package com.foogaro.kinexis.demo.cassandra;

import com.foogaro.kinexis.core.config.KinexisConfiguration;
import com.redis.om.spring.annotations.EnableRedisDocumentRepositories;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableCassandraRepositories(basePackages = "com.foogaro.kinexis.demo.cassandra.repository.cassandra")
@EnableRedisDocumentRepositories(basePackages = "com.foogaro.kinexis.demo.cassandra.repository.redis")
@EnableScheduling
@Import(KinexisConfiguration.class)
public class KinexisCassandraDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KinexisCassandraDemoApplication.class, args);
    }
}
