package io.reactiveminds.datagrid.api;

import org.apache.avro.generic.GenericRecord;

class KeyValRecord2 {

	public byte[] getKey() {
		return key;
	}
	public GenericRecord getValue() {
		return value;
	}
	public KeyValRecord2(byte[] key, GenericRecord value) {
		super();
		this.key = key;
		this.value = value;
	}
	final byte[] key;
	final GenericRecord value;
}
