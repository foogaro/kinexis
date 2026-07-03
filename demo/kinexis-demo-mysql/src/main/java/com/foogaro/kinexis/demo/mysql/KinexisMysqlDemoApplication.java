package com.foogaro.kinexis.demo.mysql;

import com.foogaro.kinexis.core.config.KinexisConfiguration;
import com.redis.om.spring.annotations.EnableRedisDocumentRepositories;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableJpaRepositories(basePackages = "com.foogaro.kinexis.demo.mysql.repository.mysql")
@EnableRedisDocumentRepositories(basePackages = "com.foogaro.kinexis.demo.mysql.repository.redis")
@EnableScheduling
@Import(KinexisConfiguration.class)
public class KinexisMysqlDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KinexisMysqlDemoApplication.class, args);
    }
}
