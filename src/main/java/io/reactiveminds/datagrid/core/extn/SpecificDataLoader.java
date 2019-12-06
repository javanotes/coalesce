package io.reactiveminds.datagrid.core.extn;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import io.reactiveminds.datagrid.api.DataLoader;
import io.reactiveminds.datagrid.util.Utils;

public abstract class SpecificDataLoader<K extends SpecificRecord,V extends SpecificRecord> extends DataLoader {

	protected abstract V load(K key);
	protected abstract Map<K, V> loadForAll(List<K> keys);
	
	@Override
	protected GenericRecord load(GenericRecord key) {
		K sKey = Utils.genericToSpecific(key);
		V val = load(sKey);
		return Utils.specificToGeneric(val);
	}

	@Override
	protected Map<GenericRecord, GenericRecord> loadAll(List<GenericRecord> keys) {
		@SuppressWarnings("unchecked")
		List<K> keySet = keys.stream().map(key -> (K) Utils.genericToSpecific(key)).collect(Collectors.toList());
		Map<K, V> result = loadForAll(keySet);
		return result.entrySet().stream().collect(Collectors.toMap(e -> Utils.specificToGeneric(e.getKey()), e -> Utils.specificToGeneric(e.getValue())));
	}

}
