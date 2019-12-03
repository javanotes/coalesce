package io.reactiveminds.datagrid.core;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;

import io.reactiveminds.datagrid.notif.EventType;
import io.reactiveminds.datagrid.spi.EventsNotifier;
import io.reactiveminds.datagrid.spi.IProcessor;
import io.reactiveminds.datagrid.vo.DataEvent;
import io.reactiveminds.datagrid.vo.ListenerConfig;

/**
 * 
 * @author sdalui
 *
 */
public class RequestListener
		implements EntryAddedListener<byte[], DataEvent>, EntryUpdatedListener<byte[], DataEvent>, EntryEvictedListener<byte[], DataEvent> {
	
	public RequestListener(ListenerConfig config) {
		this.config = config;
	}
	private final ListenerConfig config;
	private static final Logger log = LoggerFactory.getLogger("RequestListener");
	@Override
	public void entryUpdated(EntryEvent<byte[], DataEvent> event) {
		entryAdded(event);
	}
	@Autowired
	BeanFactory beans;
	@Autowired
	HazelcastInstance hz;
	@Autowired
	EventsNotifier notifier;
	@PostConstruct
	void setProcessor() {
		processor = beans.getBean(IProcessor.class, config.getKeySchema(), config.getValueSchema(), config.getCoalesceMap());
		processor.setFlushHandler(beans.getBean(config.getFlushHandler()));
		processor.setSurvivalRule(beans.getBean(config.getSurvivalRule()));
	}
	private IProcessor processor;
	@Override
	public void entryAdded(EntryEvent<byte[], DataEvent> event) {
		log.debug("----- ON_MESSAGE_RECEIPT ----");
		doProcess(event, EventType.MESSAGE_RECEIPT);
		log.debug("coalesce done ..");
	}
	IProcessor getProcessor() {
		return processor;
	}
	
	private void doProcess(EntryEvent<byte[], DataEvent> event, EventType type) {
		notifier.sendNotification(type, event.getValue(), config.getKeySchema(), config.getValueSchema());
		try {
			getProcessor().process(event.getValue());
		} finally {
			hz.getMap(config.getRequestMap()).removeAsync(event.getKey());
		}
		
	}
	@Override
	public void entryEvicted(EntryEvent<byte[], DataEvent> event) {
		doProcess(event, EventType.MESSAGE_REPEAT);
	}
}
