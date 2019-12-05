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
	public CoalesceEntry(byte[] key, byte[] value, boolean delete) {
		super();
		this.key = key;
		this.value = value;
		this.delete = delete;
	}
	final byte[] key;
	final byte[] value;
	final boolean delete;
	public boolean isDelete() {
		return delete;
	}
	
}
