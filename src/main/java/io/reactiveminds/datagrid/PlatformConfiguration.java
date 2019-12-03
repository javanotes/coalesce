package io.reactiveminds.datagrid;

import java.io.IOException;
import java.net.URL;

import org.apache.avro.Schema;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.ResourceUtils;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;

import io.reactiveminds.datagrid.api.GridContext;
import io.reactiveminds.datagrid.core.DefaultConfigRegistry;
import io.reactiveminds.datagrid.core.DefaultIngestionService;
import io.reactiveminds.datagrid.core.DefaultProcessor;
import io.reactiveminds.datagrid.core.GridContextProxy;
import io.reactiveminds.datagrid.core.RequestListener;
import io.reactiveminds.datagrid.notif.KafkaNotifier;
import io.reactiveminds.datagrid.notif.LoggingNotifier;
import io.reactiveminds.datagrid.spi.ConfigRegistry;
import io.reactiveminds.datagrid.spi.EventsNotifier;
import io.reactiveminds.datagrid.spi.IProcessor;
import io.reactiveminds.datagrid.spi.IngestionService;
import io.reactiveminds.datagrid.vo.ListenerConfig;

@Configuration
public class PlatformConfiguration implements ApplicationContextAware{
	
	@Value("${coalesce.flush.poolSize:2}")
	private int poolSize;
	@Value("${coalesce.events.poolSize:2}")
	private int tpoolSize;
	@Bean
	ConfigRegistry bootCore() {
		return new DefaultConfigRegistry();
	}
	@Bean
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	@Lazy
	RequestListener entryListener(ListenerConfig config) {
		return new RequestListener(config);
	}
	@Bean
	Config hazelcastConfig(HazelcastProperties properties) throws IOException {
		Resource configLocation = properties.resolveConfigLocation();
		URL configUrl = configLocation.getURL();
		Config config = new XmlConfigBuilder(configUrl).build();
		if (ResourceUtils.isFileURL(configUrl)) {
			config.setConfigurationFile(configLocation.getFile());
		}
		else {
			config.setConfigurationUrl(configUrl);
		}
		
		return config;
	}

	@Bean
	public ThreadPoolTaskScheduler threadPoolTaskScheduler() {
		ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
		threadPoolTaskScheduler.setBeanName("FlushSchedulers");
		threadPoolTaskScheduler.setPoolSize(poolSize);
		threadPoolTaskScheduler.setThreadNamePrefix("Coalesce.FlushScheduler-");
		return threadPoolTaskScheduler;
	}
	@Bean
	TaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
		exec.setMaxPoolSize(tpoolSize);
		exec.setThreadNamePrefix("Coalesce.EventExecutor-");
		return exec;
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
	@Value("${coalesce.core.notification:")
	private String type;
    @Bean
	EventsNotifier notifier() {
		return "kafka".equalsIgnoreCase(type) ? new KafkaNotifier() : new LoggingNotifier();
	}
    @Bean
    public ApplicationEventMulticaster applicationEventMulticaster() {
        SimpleApplicationEventMulticaster eventMulticaster =
          new SimpleApplicationEventMulticaster();
        eventMulticaster.setTaskExecutor(taskExecutor());
        return eventMulticaster;
    }
    private static ApplicationContext ctx;
    public static HazelcastInstance getHazelcast() {
    	return ctx.getBean(HazelcastInstance.class);
    }
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		ctx = applicationContext;
	}
	
}
