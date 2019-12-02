package io.reactiveminds.datagrid.api;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.hazelcast.core.MapLoader;

import io.reactiveminds.datagrid.util.Utils;
import io.reactiveminds.datagrid.vo.DataEvent;
/**
 * 
 * Extend this class to provide an instance of {@linkplain DataLoader}
 * @author sdalui
 *
 */
public abstract class DataLoader implements MapLoader<byte[], DataEvent> {
	public DataLoader() {}
	
	private Schema keySchema;
	public void setKeySchema(Schema s) {
		keySchema = s;
	}
	@Override
	public DataEvent load(byte[] key) {
		GenericRecord value = load(Utils.fromAvroBytes(key, keySchema));
		return value != null ? toEvent(key, value) : null;
	}
	private static DataEvent toEvent(byte[] key, GenericRecord value) {
		DataEvent v = new DataEvent();
		v.setMessageValue(Utils.toAvroBytes(value));
		v.setMessageKey(key);
		v.setKeyCheksum(Utils.generateKeyChecksum(v.getMessageKey()));
		v.setValueCheksum(Utils.generateValueChecksum(v.getMessageValue()));
		v.setLoaded(true);
		return v;
	}
	@Override
	public Map<byte[], DataEvent> loadAll(Collection<byte[]> keys) {
		List<GenericRecord> keyset = keys.stream().map(b -> Utils.fromAvroBytes(b, keySchema))
				.collect(Collectors.toList());
		 
		if(keyset == null || keyset.isEmpty())
			return Collections.emptyMap();
		
		return loadAll(keyset)
		 .entrySet()
		 .stream()
		 .map(e -> new KeyValRecord2(Utils.toAvroBytes(e.getKey()), e.getValue()) )
		.collect(Collectors.toMap(k -> k.getKey(), k -> toEvent(k.getKey(), k.getValue())));
	}

	@Override
	public Iterable<byte[]> loadAllKeys() {
		return null;
	}
	/**
	 * Load a new record given its key
	 * @param key
	 * @return
	 */
	protected abstract GenericRecord load(GenericRecord key);
	/**
	 * Load records passed multiple keys
	 * @param keys
	 * @return
	 */
	protected abstract Map<GenericRecord, GenericRecord> loadAll(List<GenericRecord> keys);
}
