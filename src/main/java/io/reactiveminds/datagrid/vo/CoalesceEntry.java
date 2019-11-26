package io.reactiveminds.datagrid.vo;

public class CoalesceEntry {

	public byte[] getKey() {
		return key;
	}
	public byte[] getValue() {
		return value;
	}
	/**
	 * 
	 * @param key
	 * @param value
	 */
	public CoalesceEntry(byte[] key, byte[] value) {
		super();
		this.key = key;
		this.value = value;
	}
	final byte[] key;
	final byte[] value;
}
