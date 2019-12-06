package io.reactiveminds.datagrid.spi;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
public interface IngestionService {
	/**
	 * 
	 * @param requestMap
	 * @param key
	 * @param value
	 * @param inTime
	 * @return 
	 */
	String ingest(String requestMap, GenericRecord key, GenericRecord value, long inTime);
	<V extends SpecificRecord, K extends SpecificRecord> String ingest(String requestMap, K key, V value, long inTime);
}
