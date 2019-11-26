package io.reactiveminds.datagrid.spi;

import org.apache.avro.Schema;

import io.reactiveminds.datagrid.notif.EventType;
import io.reactiveminds.datagrid.vo.DataEvent;
import io.reactiveminds.datagrid.vo.KeyValRecord;

public interface EventsNotifier {

	/**
	 * 
	 * @param event
	 * @param rec
	 * @return
	 */
	void sendNotification(EventType event, KeyValRecord rec, String traceId);

	/**
	 * 
	 * @param event
	 * @param traceId
	 */
	void sendNotification(EventType event, String traceId);

	/**
	 * 
	 * @param event
	 * @param rec
	 * @param k
	 * @param v
	 * @return
	 */
	void sendNotification(EventType event, DataEvent rec, Schema k, Schema v);

}