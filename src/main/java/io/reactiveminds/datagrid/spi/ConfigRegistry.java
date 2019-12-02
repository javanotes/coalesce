package io.reactiveminds.datagrid.spi;

import org.apache.avro.Schema;
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
	 * @param listenerConfig
	 * @return
	 */
	String getCoalesceMap(String listenerConfig);
	/**
	 * 
	 * @param key
	 * @return
	 */
	String getTracingId(GenericRecord k);
	/**
	 * 
	 * @param listenerConfig
	 * @return
	 */
	Schema getKeySchema(String listenerConfig);
	/**
	 * 
	 * @param listenerConfig
	 * @return
	 */
	Schema getValueSchema(String listenerConfig);
	/**
	 * 
	 * @param listenerConfig
	 */
	IProcessor getProcessor(String listenerConfig);
}