package io.reactiveminds.datagrid.notif;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;
import org.springframework.util.Assert;

import io.reactiveminds.datagrid.PlatformConfiguration;
import io.reactiveminds.datagrid.spi.EventsNotifier;
import io.reactiveminds.datagrid.vo.DataEvent;
import io.reactiveminds.datagrid.vo.KeyValRecord;

public abstract class AbstractEventsNotifier implements ApplicationListener<EventNotification>, ApplicationEventPublisherAware, EventsNotifier{
	private ApplicationEventPublisher applicationEventPublisher;
	
	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}
	private String sourceId;
	@PostConstruct
	void init() {
		sourceId = PlatformConfiguration.getHazelcast().getLocalEndpoint().getUuid();
	}
	/**
	 * 
	 * @param event
	 * @param rec
	 * @return
	 */
	@Override
	public void sendNotification(EventType event, KeyValRecord rec, String traceId) {
		EventNotification notif = new EventNotification(sourceId);
		notif.setEvent(event);
		if (rec != null) {
			Assert.notNull(rec.getKey(), "'key' reqd for notification");
			notif.setKey(rec.getKey().toString());
			if (rec.getValue() != null) {
				notif.setValue(rec.getValue().toString());
			}
		}
		notif.setTracingId(traceId);
		applicationEventPublisher.publishEvent(notif);
	}
	/**
	 * 
	 * @param event
	 * @param traceId
	 */
	@Override
	public void sendNotification(EventType event, String traceId) {
		sendNotification(event, null, traceId);
	}
	/**
	 * 
	 * @param event
	 * @param rec
	 * @param k
	 * @param v
	 * @return
	 */
	@Override
	public void sendNotification(EventType event, DataEvent rec, Schema k, Schema v) {
		sendNotification(event, null, rec.getUid());
	}
}