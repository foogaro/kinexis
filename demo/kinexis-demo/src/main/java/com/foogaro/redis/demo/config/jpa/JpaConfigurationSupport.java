package com.foogaro.redis.demo.config.jpa;

import jakarta.persistence.EntityManagerFactory;
import org.hibernate.cfg.AvailableSettings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

abstract class JpaConfigurationSupport {

    private static final String ENTITY_PACKAGE = "com.foogaro.redis.demo.entity";

    @Value("${spring.jpa.hibernate.ddl-auto:update}")
    private String ddlAuto;

    @Value("${spring.jpa.properties.hibernate.jdbc.time_zone:UTC}")
    private String jdbcTimeZone;

    @Value("${spring.jpa.show-sql:false}")
    private boolean showSql;

    @Value("${spring.jpa.properties.hibernate.format_sql:false}")
    private boolean formatSql;

    protected LocalContainerEntityManagerFactoryBean entityManagerFactory(
            DataSource dataSource,
            String persistenceUnitName,
            String dialect) {
        LocalContainerEntityManagerFactoryBean factoryBean = new LocalContainerEntityManagerFactoryBean();
        factoryBean.setDataSource(dataSource);
        factoryBean.setPackagesToScan(ENTITY_PACKAGE);
        factoryBean.setPersistenceUnitName(persistenceUnitName);
        factoryBean.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
        factoryBean.setJpaPropertyMap(jpaProperties(dialect));
        return factoryBean;
    }

    protected PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    private Map<String, Object> jpaProperties(String dialect) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(AvailableSettings.HBM2DDL_AUTO, ddlAuto);
        properties.put(AvailableSettings.DIALECT, dialect);
        properties.put(AvailableSettings.JDBC_TIME_ZONE, jdbcTimeZone);
        properties.put(AvailableSettings.SHOW_SQL, showSql);
        properties.put(AvailableSettings.FORMAT_SQL, formatSql);
        return properties;
    }
}
