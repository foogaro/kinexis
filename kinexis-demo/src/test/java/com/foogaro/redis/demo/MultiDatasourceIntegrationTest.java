package com.foogaro.redis.demo;

import com.foogaro.redis.demo.config.jpa.MysqlJpaConfiguration;
import com.foogaro.redis.demo.config.jpa.PostgresJpaConfiguration;
import com.foogaro.redis.demo.config.jpa.SqlServerJpaConfiguration;
import com.foogaro.redis.demo.entity.Employer;
import com.foogaro.redis.demo.repository.mysql.EmployerMysqlRepository;
import com.foogaro.redis.demo.repository.psql.EmployerPostgresRepository;
import com.foogaro.redis.demo.repository.sqlserver.EmployerSqlServerRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@SpringBootTest(classes = MultiDatasourceIntegrationTest.TestApplication.class)
class MultiDatasourceIntegrationTest {

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>(DockerImageName.parse("mysql:8.4"))
            .withDatabaseName("kinexis")
            .withUsername("kinexis")
            .withPassword("kinexis");

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("kinexis")
            .withUsername("kinexis")
            .withPassword("kinexis");

    @Container
    private static final MSSQLServerContainer<?> SQL_SERVER = new MSSQLServerContainer<>(
            DockerImageName.parse("mcr.microsoft.com/mssql/server:2022-latest"))
            .acceptLicense();

    @DynamicPropertySource
    static void configureDatasources(DynamicPropertyRegistry registry) {
        registry.add("kinexis.datasource.mysql.url", MYSQL::getJdbcUrl);
        registry.add("kinexis.datasource.mysql.username", MYSQL::getUsername);
        registry.add("kinexis.datasource.mysql.password", MYSQL::getPassword);
        registry.add("kinexis.datasource.mysql.driver-class-name", MYSQL::getDriverClassName);

        registry.add("kinexis.datasource.postgres.url", POSTGRES::getJdbcUrl);
        registry.add("kinexis.datasource.postgres.username", POSTGRES::getUsername);
        registry.add("kinexis.datasource.postgres.password", POSTGRES::getPassword);
        registry.add("kinexis.datasource.postgres.driver-class-name", POSTGRES::getDriverClassName);

        registry.add("kinexis.datasource.sqlserver.url", () -> SQL_SERVER.getJdbcUrl() + ";encrypt=false;trustServerCertificate=true");
        registry.add("kinexis.datasource.sqlserver.username", SQL_SERVER::getUsername);
        registry.add("kinexis.datasource.sqlserver.password", SQL_SERVER::getPassword);
        registry.add("kinexis.datasource.sqlserver.driver-class-name", SQL_SERVER::getDriverClassName);

        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @Autowired
    private EmployerMysqlRepository mysqlRepository;

    @Autowired
    private EmployerPostgresRepository postgresRepository;

    @Autowired
    private EmployerSqlServerRepository sqlServerRepository;

    @Test
    void mysqlPostgresAndSqlServerRepositoriesUseSeparateDatasources() {
        Employer mysqlEmployer = employer(101L, "MySQL Employer", "mysql@kinexis.local");
        Employer postgresEmployer = employer(202L, "PostgreSQL Employer", "postgres@kinexis.local");
        Employer sqlServerEmployer = employer(303L, "SQL Server Employer", "sqlserver@kinexis.local");

        mysqlRepository.save(mysqlEmployer);
        postgresRepository.save(postgresEmployer);
        sqlServerRepository.save(sqlServerEmployer);

        assertEquals("MySQL Employer", mysqlRepository.findById(101L).orElseThrow().getName());
        assertTrue(mysqlRepository.findById(202L).isEmpty());
        assertTrue(mysqlRepository.findById(303L).isEmpty());

        assertEquals("PostgreSQL Employer", postgresRepository.findById(202L).orElseThrow().getName());
        assertTrue(postgresRepository.findById(101L).isEmpty());
        assertTrue(postgresRepository.findById(303L).isEmpty());

        assertEquals("SQL Server Employer", sqlServerRepository.findById(303L).orElseThrow().getName());
        assertTrue(sqlServerRepository.findById(101L).isEmpty());
        assertTrue(sqlServerRepository.findById(202L).isEmpty());
    }

    private static Employer employer(Long id, String name, String email) {
        Employer employer = new Employer();
        employer.setId(id);
        employer.setName(name);
        employer.setAddress(name + " Address");
        employer.setEmail(email);
        employer.setPhone("+39000" + id);
        return employer;
    }

    @SpringBootConfiguration
    @EnableConfigurationProperties
    @Import({
            MysqlJpaConfiguration.class,
            PostgresJpaConfiguration.class,
            SqlServerJpaConfiguration.class
    })
    static class TestApplication {
    }
}
