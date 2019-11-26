package io.reactiveminds.datagrid.core;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import io.reactiveminds.datagrid.notif.EventType;
import io.reactiveminds.datagrid.spi.EventsNotifier;
import io.reactiveminds.datagrid.spi.IngestionService;
import io.reactiveminds.datagrid.util.Utils;
import io.reactiveminds.datagrid.vo.DataEvent;

class DefaultIngestionService implements IngestionService {

	private static final Logger log = LoggerFactory.getLogger("DefaultIngestionService");
	@Autowired
	HazelcastInstance hz;
	@Autowired
	EventsNotifier notifier;
	@Override
	public void apply(String requestMap, GenericRecord key, GenericRecord value, long inTime) {
		IMap<byte[], DataEvent> map = hz.getMap(requestMap);
		DataEvent event = new DataEvent();
		event.setMessageKey(Utils.toAvroBytes(key));
		event.setMessageValue(Utils.toAvroBytes(value));
		event.setKeyCheksum(Utils.generateKeyChecksum(event.getMessageKey()));
		event.setValueCheksum(Utils.generateValueChecksum(event.getMessageValue()));
		event.setOriginTime(inTime);
		event.setIngressTime(System.currentTimeMillis());
		notifier.sendNotification(EventType.MESSAGE_CREATE, event.getKeyCheksum());
		
		map.set(event.getMessageKey(), event);//TODO: ttl?
		log.info("acknowledge to request map: "+requestMap);
	}

}