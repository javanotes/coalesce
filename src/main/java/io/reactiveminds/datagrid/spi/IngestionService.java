package io.reactiveminds.datagrid.spi;

import org.apache.avro.generic.GenericRecord;
@FunctionalInterface
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
}
