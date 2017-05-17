package com.ge.current;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.JpaVendorAdapter;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.OpenJpaVendorAdapter;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Configuration
@ComponentScan(basePackages = "com.ge.current.em.aggregation," +
		                      "com.ge.current.em.analytics.common," +
							  "com.ge.current.ie.analytics," +
                              "com.ge.current.em.persistencecore.repository," +
                              "com.ge.current.em.persistencecore.entity," +
                              "com.ge.current.em.persistencecore.service"
)
@EnableJpaRepositories(basePackages = "com.ge", entityManagerFactoryRef = "entityManager")
public class EMConfigurations {

	@Value(value="${apm.db.schema}")
	private String apmDbSchema;

	@Value(value="${apm.db.url}")
	private String apmDbUrl;

	@Value(value="${apm.db.username}")
	private String apmDbUsername;

	@Value(value="${apm.db.password}")
	private String apmDbPassword;

	@Bean
	public DriverManagerDataSource dataSource() {
		DriverManagerDataSource ds = new DriverManagerDataSource();
		ds.setDriverClassName("org.postgresql.Driver");
		ds.setUrl(apmDbUrl);
		ds.setUsername(apmDbUsername);
		ds.setPassword(apmDbPassword);

		return ds;
	}

	@Bean
	public LocalContainerEntityManagerFactoryBean entityManager() {
		LocalContainerEntityManagerFactoryBean entityManager = new LocalContainerEntityManagerFactoryBean();
		entityManager.setDataSource(dataSource());
		entityManager.setJpaVendorAdapter(jpaVendorAdapter());
		entityManager.setPackagesToScan("com.ge.current.em.persistencecore.entity.sql");
		// entityManager.setPersistenceUnitName("classpath:");
		entityManager.setJpaProperties(getJpaProperties());
		return entityManager;
	}

	private Properties getJpaProperties() {
		Properties properties = new Properties();
		properties.setProperty("openjpa.jdbc.Schema", apmDbSchema);
		return properties;
	}

	@Bean
	public JpaVendorAdapter jpaVendorAdapter() {
		OpenJpaVendorAdapter openJpaVendorAdapter = new OpenJpaVendorAdapter();
		openJpaVendorAdapter.setShowSql(false);
		openJpaVendorAdapter.setGenerateDdl(false);
		openJpaVendorAdapter.setDatabase(Database.POSTGRESQL);
		return openJpaVendorAdapter;
	}


	@Bean
	public List<org.springframework.web.servlet.mvc.method.RequestMappingInfoHandlerMapping> getDummy() {
		List<org.springframework.web.servlet.mvc.method.RequestMappingInfoHandlerMapping> list = new ArrayList<>();
		// list.add(RequestMappingInfoHandlerMapping.HIGHEST_PRECEDENCE);
		return list;
	}

	@Bean
	public javax.servlet.ServletContext getContext(){
		return new DummyServletContext();
	}
}