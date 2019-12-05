package io.reactiveminds.datagrid.core;

import org.apache.avro.Schema;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;

import io.reactiveminds.datagrid.api.GridContext;
import io.reactiveminds.datagrid.spi.ConfigRegistry;
import io.reactiveminds.datagrid.spi.IProcessor;
import io.reactiveminds.datagrid.spi.IngestionService;
import io.reactiveminds.datagrid.vo.ListenerConfig;

@Configuration
public class CoreConfiguration {

	@Bean
	ConfigRegistry bootCore() {
		return new ProcessorEngine();
	}
	@Bean
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	@Lazy
	RequestListener entryListener(ListenerConfig config) {
		return new RequestListener(config);
	}
	@Bean
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	@Lazy
	IProcessor processor(Schema keySchema, Schema valSchema, String imap) {
		return new DefaultProcessor(keySchema, valSchema, imap);
	}
	@Bean
	IngestionService ingestService() {
		return new DefaultIngestionService();
	}
	@Bean 
	GridContext grid() {
		return new GridContextProxy();
	}
}
