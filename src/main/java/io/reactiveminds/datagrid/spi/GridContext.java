package io.reactiveminds.datagrid.spi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public interface GridContext {
	/**
	 * 
	 * @param mapName
	 * @param key
	 * @return
	 */
	boolean containsKey(String mapName, GenericRecord key);
	/**
	 * 
	 * @param mapName
	 * @param key
	 * @param valueSchema
	 * @return
	 */
	GenericRecord getValue(String mapName, GenericRecord key, Schema valueSchema);
}
