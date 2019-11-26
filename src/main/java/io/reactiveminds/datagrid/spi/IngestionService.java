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
	 */
	void apply(String requestMap, GenericRecord key, GenericRecord value, long inTime);
}
