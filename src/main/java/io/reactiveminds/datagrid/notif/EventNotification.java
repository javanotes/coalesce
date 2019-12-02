package io.reactiveminds.datagrid.notif;

import org.springframework.context.ApplicationEvent;

public class EventNotification extends ApplicationEvent{

	
	@Override
	public String toString() {
		return " [event=" + event + ", key=" + key + ", value=" + (value == null ? "" : "{suppressed ..}") + ", generatedBy="
				+ getSource() + ", tracingId=" + tracingId + "]";
	}
	public EventNotification(Object source) {
		super(source);
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = -2790279289131712950L;
	public EventType getEvent() {
		return event;
	}
	public void setEvent(EventType event) {
		this.event = event;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
	EventType event;
	String key;
	String value;
	String tracingId;
	public String getTracingId() {
		return tracingId;
	}
	public void setTracingId(String tracingId) {
		this.tracingId = tracingId;
	}
}
