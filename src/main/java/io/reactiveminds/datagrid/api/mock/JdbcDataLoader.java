package io.reactiveminds.datagrid.api.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import io.reactiveminds.datagrid.api.DataLoader;
import io.reactiveminds.datagrid.err.DataStoreException;
import io.reactiveminds.datagrid.util.Utils;
@Deprecated
public class JdbcDataLoader extends DataLoader {

	String selectQuery;
	String[] keyMapping;
	Schema valueSchema;
	
	
	private Object[] bind(GenericRecord key) {
		Object[] args = new Object[keyMapping.length];
		for (int i = 0; i < args.length; i++) {
			args[i] = key.get(keyMapping[i]);
		}
		return args;
	}
	@Autowired
	JdbcTemplate template;
	@Override
	protected GenericRecord load(GenericRecord key) {
		try {
			String clob = template.queryForObject(selectQuery, bind(key), String.class);
			return Utils.jsonToAvroRecord(valueSchema, clob);
		} catch (Exception e) {
			throw new DataStoreException("Load for key failed: "+key, e);
		}
	}

	@Override
	protected Map<GenericRecord, GenericRecord> loadAll(List<GenericRecord> keys) {
		Map<GenericRecord, GenericRecord> result = new HashMap<>();
		for(GenericRecord key: keys) {
			result.put(key, load(key));
		}
		return result;
	}

}
