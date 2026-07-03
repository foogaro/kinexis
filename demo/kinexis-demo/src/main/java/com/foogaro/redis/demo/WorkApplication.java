package com.foogaro.redis.demo;

import com.foogaro.kinexis.core.config.KinexisConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableRedisRepositories(basePackages = "com.foogaro.redis.demo.entity")
@EnableMongoRepositories(basePackages = "com.foogaro.redis.demo.repository.mongodb")
@EnableCassandraRepositories(basePackages = "com.foogaro.redis.demo.repository.cassandra")
@EnableScheduling
@Import(KinexisConfiguration.class)  // Add this
public class WorkApplication {

	public static void main(String[] args) {
		SpringApplication.run(WorkApplication.class, args);
	}

}
