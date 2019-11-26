package io.reactiveminds.datagrid.spi;

import org.apache.avro.generic.GenericRecord;

public interface ConfigRegistry {
	/**
	 * 
	 * @param listenerConfig
	 * @return Name of IMap
	 */
	String getRequestMap(String listenerConfig);
	/**
	 * 
	 * @param k
	 * @return
	 */
	String getTracingId(GenericRecord k);
}