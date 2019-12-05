package io.reactiveminds.datagrid;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.ConsumerAwareMessageListener;

import io.reactiveminds.datagrid.spi.IngestionService;
class IngestionConsumer implements ConsumerAwareMessageListener<GenericRecord, GenericRecord> {
	private static final Logger log = LoggerFactory.getLogger("RequestConsumer");
	@Autowired
	IngestionService ingestService;
	
	private final String requestMap;
	@Value("${coalesce.core.requestMapTimeToLiveSecs:30}")
	private long ttl;
	
	public IngestionConsumer(String imap) {
		this.requestMap = imap;
	}

	@Override
	public void onMessage(ConsumerRecord<GenericRecord, GenericRecord> data, Consumer<?, ?> consumer) {
		ingestService.apply(requestMap, data.key(), data.value(), data.timestamp());
		log.info("acknowledge to request map: "+requestMap);
	}

}
