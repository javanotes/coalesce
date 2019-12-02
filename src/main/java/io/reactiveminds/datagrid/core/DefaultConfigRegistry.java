package io.reactiveminds.datagrid.core;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.util.StringUtils;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;

import io.reactiveminds.datagrid.api.DataLoader;
import io.reactiveminds.datagrid.err.ConfigurationException;
import io.reactiveminds.datagrid.spi.ConfigRegistry;
import io.reactiveminds.datagrid.spi.IProcessor;
import io.reactiveminds.datagrid.util.ThrowingLambdaFunc;
import io.reactiveminds.datagrid.util.Utils;
import io.reactiveminds.datagrid.vo.ListenerConfig;

class DefaultConfigRegistry implements DisposableBean, ConfigRegistry{

	private static final Logger log = LoggerFactory.getLogger("DefaultConfigRegistry");
	@Autowired
	ListableBeanFactory beans;
	@Autowired
	HazelcastInstance hz;
	@Autowired
	TaskScheduler scheduler;
	
	@Autowired
	Config config;
	@Value("${coalesce.core.listenerConfigDir}")
	private String listenerConfigDir;
	
	private class ListenerContainer implements Runnable,DisposableBean{

		private String hzListenerId;
		private MessageListenerContainer container;
		private final ListenerConfig cfg;
		private ScheduledFuture<?> future;
		String getConfigName() {
			return cfg.getName();
		}
		IProcessor getProcessor() {
			return listener.getProcessor();
		}
		public ListenerContainer(ListenerConfig cfg) 
		{
			this.cfg = cfg;
			try {
				checkProperties();
				checkDependencies();
			} 
			catch (IllegalArgumentException e) {
				throw new ConfigurationException("["+cfg.getName() + "] "+e.getMessage());
			}
			
			DataLoader loader = beans.getBean(cfg.getDataLoader());
			loader.setKeySchema(cfg.getKeySchema());
			
			MapStoreConfig mapstore = config.getMapConfig(cfg.getCoalesceMap()).getMapStoreConfig();
			mapstore.setEnabled(true);
			mapstore.setImplementation(loader);
			
			if (StringUtils.hasText(cfg.getRequestTopic())) {
				container = beans.getBean(MessageListenerContainer.class, cfg.getRequestTopic(),
						cfg.getRequestMap());
			}
			
		}

		private void checkProperties() {
			cfg.assertNotNull();
		}

		private void checkDependencies() {
			String [] b = beans.getBeanNamesForType(cfg.getDataLoader());
			if(b.length == 0)
				throw new IllegalArgumentException("No matching dataLoader bean found: "+ cfg.getDataLoader());
			b = beans.getBeanNamesForType(cfg.getFlushHandler());
			if(b.length == 0)
				throw new IllegalArgumentException("No matching flushHandler bean found: "+ cfg.getFlushHandler());
			b = beans.getBeanNamesForType(cfg.getSurvivalRule());
			if(b.length == 0)
				throw new IllegalArgumentException("No matching survivalRule bean found: "+ cfg.getSurvivalRule());
		}

		private RequestListener listener;
		@Override
		public void run() {
			listener = beans.getBean(RequestListener.class, cfg);
			hzListenerId = hz.getMap(cfg.getRequestMap()).addLocalEntryListener(listener);
			
			if (container != null) {
				container.start();
			}
			future = scheduler.schedule(() -> {
				try {
					listener.getProcessor().flush();
				} catch (IOException e) {
					log.error("Uncaught exception in flush", e);
				}
			}, new CronTrigger(cfg.getFlushSchedule()));
			
			log.info("|[ "+cfg.getName()+" ]| Processor running: " +(container != null ? " with incoming topic - "+cfg.getRequestTopic()+", " : "")
					+ "listening on entry map - "+cfg.getRequestMap()+" for coalesce map - "+cfg.getCoalesceMap());
		}

		@Override
		public void destroy() {
			if (container != null) {
				container.stop();
			}
			hz.getMap(cfg.getRequestMap()).removeEntryListener(hzListenerId);
			future.cancel(true);
		}
		
	}
	@Override
	public String getRequestMap(String listenerConfig) {
		if(configMap.containsKey(listenerConfig)) {
			return configMap.get(listenerConfig).getRequestMap();
		}
		return null;
	}
	private final List<ListenerContainer> listenerRegistry = new LinkedList<>();
	private Map<String, ListenerConfig> configMap;
	private Map<String, ListenerContainer> listenerMap;
	/**
	 * 
	 */
	public void destroy() {
		listenerRegistry.forEach(ListenerContainer::destroy);
	}
	@PostConstruct
	void setup() {
		
		listenerRegistry.addAll(ListenerConfig.loadAll(listenerConfigDir).stream().map( ThrowingLambdaFunc.throwsConfigurationException(ListenerContainer::new, "Invalid listener configuration") ).collect(Collectors.toList()));
		configMap = Collections.unmodifiableMap( listenerRegistry.stream().collect(Collectors.toMap(l -> l.cfg.getName(), l -> l.cfg)) );
		listenerMap = listenerRegistry.stream().collect(Collectors.toMap(ListenerContainer::getConfigName, Function.identity()));
				
		for(ListenerContainer config : listenerRegistry) {
			config.run();
		}
	}
	@Override
	public String getTracingId(GenericRecord k) {
		return Utils.generateKeyChecksum(Utils.toAvroBytes(k));
	}
	@Override
	public Schema getKeySchema(String listenerConfig) {
		if(configMap.containsKey(listenerConfig)) {
			return configMap.get(listenerConfig).getKeySchema();
		}
		return null;
	}
	@Override
	public String getCoalesceMap(String listenerConfig) {
		if(configMap.containsKey(listenerConfig)) {
			return configMap.get(listenerConfig).getCoalesceMap();
		}
		return null;
	}
	@Override
	public Schema getValueSchema(String listenerConfig) {
		if(configMap.containsKey(listenerConfig)) {
			return configMap.get(listenerConfig).getValueSchema();
		}
		return null;
	}
	@Override
	public IProcessor getProcessor(String listenerConfig) {
		if(listenerMap.containsKey(listenerConfig)) {
			return listenerMap.get(listenerConfig).getProcessor();
		}
		return null;
	}
}
