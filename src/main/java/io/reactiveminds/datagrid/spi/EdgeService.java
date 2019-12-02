package io.reactiveminds.datagrid.spi;

import org.apache.avro.generic.GenericRecord;

import io.reactiveminds.datagrid.vo.DataEvent;

public interface EdgeService {
	/**
	 * 
	 * @param imap
	 * @param key
	 * @return
	 */
	DataEvent get(String imap, GenericRecord key);
	/**
	 * 
	 * @param imap
	 * @param jsonPathQry
	 * @return
	 */
	String search(String imap, String jsonPathQry);
	/**
	 * 
	 * @param listenerCfg
	 */
	void flush(String listenerCfg);
}
