package io.reactiveminds.datagrid.api.mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.config.JdbcConfiguration;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.ResourceUtils;

@Configuration
@EnableJdbcRepositories
public class JdbcStoreConfiguration extends JdbcConfiguration{

	@Bean
    NamedParameterJdbcOperations operations(DataSource dataSource) { 
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean
    PlatformTransactionManager transactionManager(DataSource dataSource) { 
        return new DataSourceTransactionManager(dataSource);
	}
    @PostConstruct
    void init() throws ClassNotFoundException, IOException {
    	findAnnotatedClasses(getClass().getPackage().getName());
    	File f = ResourceUtils.getFile("classpath:schema.sql");
    	String schemaDdl = Files.readAllLines(f.toPath()).stream().collect(Collectors.joining());
    	
    	jdbc.execute(schemaDdl);
    }
    @Autowired
    JdbcTemplate jdbc;
    private void findAnnotatedClasses(String scanPackage) throws ClassNotFoundException {
        ClassPathScanningCandidateComponentProvider provider = createComponentScanner();
        for (BeanDefinition beanDef : provider.findCandidateComponents(scanPackage)) {
            Class.forName(beanDef.getBeanClassName());
        }
    }
 
    private ClassPathScanningCandidateComponentProvider createComponentScanner() {
        // Don't pull default filters (@Component, etc.):
        ClassPathScanningCandidateComponentProvider provider
                = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AnnotationTypeFilter(Table.class));
        return provider;
    }
}
