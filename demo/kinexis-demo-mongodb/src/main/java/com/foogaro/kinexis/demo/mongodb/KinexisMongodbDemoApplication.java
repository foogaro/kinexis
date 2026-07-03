package com.foogaro.kinexis.demo.mongodb;

import com.foogaro.kinexis.core.config.KinexisConfiguration;
import com.redis.om.spring.annotations.EnableRedisDocumentRepositories;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableMongoRepositories(basePackages = "com.foogaro.kinexis.demo.mongodb.repository.mongodb")
@EnableRedisDocumentRepositories(basePackages = "com.foogaro.kinexis.demo.mongodb.repository.redis")
@EnableScheduling
@Import(KinexisConfiguration.class)
public class KinexisMongodbDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KinexisMongodbDemoApplication.class, args);
    }
}
