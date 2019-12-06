package io.reactiveminds.datagrid.vo;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import io.reactiveminds.datagrid.util.Utils;

public class SpecificKeyValRecord<K extends SpecificRecord,V extends SpecificRecord> extends KeyValRecord {

	public SpecificKeyValRecord(KeyValRecord proxy) {
		this(proxy.getKey(), proxy.getValue());
	}
	private SpecificKeyValRecord(GenericRecord key, GenericRecord value) {
		super(key, value);
	}

	public K getSpecificKey() {
		return Utils.genericToSpecific(getKey());
	}
	public V getSpecificValue() {
		return Utils.genericToSpecific(getValue());
	}
}
