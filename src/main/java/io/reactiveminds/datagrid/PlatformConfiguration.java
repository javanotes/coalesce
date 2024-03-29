package io.reactiveminds.datagrid;

import java.io.IOException;
import java.net.URL;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
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

import io.reactiveminds.datagrid.core.CoreConfiguration;
import io.reactiveminds.datagrid.notif.KafkaNotifier;
import io.reactiveminds.datagrid.notif.LoggingNotifier;
import io.reactiveminds.datagrid.spi.EventsNotifier;

@Import(CoreConfiguration.class)
@Configuration
public class PlatformConfiguration implements ApplicationContextAware{
	
	@Value("${coalesce.flush.poolSize:2}")
	private int poolSize;
	@Value("${coalesce.events.poolSize:2}")
	private int tpoolSize;
	
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
