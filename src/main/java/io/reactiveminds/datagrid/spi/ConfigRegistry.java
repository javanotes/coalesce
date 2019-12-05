package io.reactiveminds.datagrid.spi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import io.reactiveminds.datagrid.vo.GridCommand;

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
	String getKeyTracingId(GenericRecord k);
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
	 * Broadcast a cluster level command to be followed by each peer
	 * @param command
	 */
	void submitCommand(GridCommand command);
}