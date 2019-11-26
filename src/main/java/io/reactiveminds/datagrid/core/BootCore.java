package io.reactiveminds.datagrid.core;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

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
import io.reactiveminds.datagrid.util.ThrowingLambdaFunc;
import io.reactiveminds.datagrid.util.Utils;
import io.reactiveminds.datagrid.vo.ListenerConfig;

class BootCore implements DisposableBean, ConfigRegistry{

	private static final Logger log = LoggerFactory.getLogger("BootCore");
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
	
	private class ListenerSetup implements Runnable,DisposableBean{

		private String id;
		private MessageListenerContainer container;
		private final ListenerConfig cfg;
		private ScheduledFuture<?> future;
		
		public ListenerSetup(ListenerConfig cfg) 
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

		@Override
		public void run() {
			RequestListener listener = beans.getBean(RequestListener.class, cfg);
			id = hz.getMap(cfg.getRequestMap()).addLocalEntryListener(listener);
			
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
			
			log.info("["+cfg.getName()+"] Processor running:" +(container != null ? " with incoming topic - "+cfg.getRequestTopic()+", " : "")
					+ "listening on entry map - "+cfg.getRequestMap()+" for coalesce map - "+cfg.getCoalesceMap());
		}

		@Override
		public void destroy() {
			if (container != null) {
				container.stop();
			}
			hz.getMap(cfg.getRequestMap()).removeEntryListener(id);
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
	private final List<ListenerSetup> configs = new LinkedList<>();
	private Map<String, ListenerConfig> configMap;
	/**
	 * 
	 */
	public void destroy() {
		configs.forEach(ListenerSetup::destroy);
	}
	@PostConstruct
	void setup() {
		
		configs.addAll(ListenerConfig.loadAll(listenerConfigDir).stream().map( ThrowingLambdaFunc.throwsConfigurationException(ListenerSetup::new, "Invalid listener configuration") ).collect(Collectors.toList()));
		configMap = Collections.unmodifiableMap( configs.stream().collect(Collectors.toMap(l -> l.cfg.getName(), l -> l.cfg)) );
		
		for(ListenerSetup config : configs) {
			config.run();
		}
	}
	@Override
	public String getTracingId(GenericRecord k) {
		return Utils.generateKeyChecksum(Utils.toAvroBytes(k));
	}
}
