package io.reactiveminds.datagrid.core;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import io.reactiveminds.datagrid.spi.GridContext;
import io.reactiveminds.datagrid.util.Utils;
import io.reactiveminds.datagrid.vo.DataEvent;

class GridContextProxy implements GridContext {

	@Autowired
	HazelcastInstance hz;
	
	@Override
	public boolean containsKey(String mapName, GenericRecord key) {
		IMap<byte[], DataEvent> map = hz.getMap(mapName);
		return map.containsKey(Utils.toAvroBytes((GenericRecord) key));
	}
	@Override
	public GenericRecord getValue(String mapName, GenericRecord key, Schema valueSchema) {
		IMap<byte[], DataEvent> map = hz.getMap(mapName);
		DataEvent v = map.get(Utils.toAvroBytes((GenericRecord) key));
		return v != null ? Utils.fromAvroBytes(v.getMessageValue(), valueSchema) : null;
	}

}
