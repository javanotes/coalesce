package io.reactiveminds.datagrid.vo;

import org.apache.avro.generic.GenericRecord;

public class KeyValRecord {

	@Override
	public String toString() {
		return "[key=" + key + ", value=" + value + "]";
	}
	public GenericRecord getKey() {
		return key;
	}
	public GenericRecord getValue() {
		return value;
	}
	public KeyValRecord(GenericRecord key, GenericRecord value) {
		super();
		this.key = key;
		this.value = value;
	}
	final GenericRecord key;
	final GenericRecord value;
}
