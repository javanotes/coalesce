package io.reactiveminds.datagrid.vo;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class DataEvent implements DataSerializable {

	public byte[] getMessageKey() {
		return messageKey;
	}

	public void setMessageKey(byte[] messageKey) {
		this.messageKey = messageKey;
	}

	@Override
	public String toString() {
		return "DataEvent [uid=" + uid + ", size="
				+ size() + ", keyCheksum=" + keyCheksum + ", valueCheksum=" + valueCheksum
				+ ", originTime=" + originTime + ", ingressTime=" + ingressTime + ", loaded=" + loaded + "]";
	}

	public byte[] getMessageValue() {
		return messageValue;
	}

	public void setMessageValue(byte[] messageValue) {
		this.messageValue = messageValue;
	}

	public String getKeyCheksum() {
		return keyCheksum;
	}

	public void setKeyCheksum(String keyCheksum) {
		this.keyCheksum = keyCheksum;
	}

	public String getValueCheksum() {
		return valueCheksum;
	}

	public void setValueCheksum(String valueCheksum) {
		this.valueCheksum = valueCheksum;
	}

	public long getOriginTime() {
		return originTime;
	}

	public void setOriginTime(long originTime) {
		this.originTime = originTime;
	}

	public long getIngressTime() {
		return ingressTime;
	}

	public void setIngressTime(long ingressTime) {
		this.ingressTime = ingressTime;
	}

	private byte[] messageKey;
	private byte[] messageValue;
	private String keyCheksum;
	private String valueCheksum;
	private long originTime;
	private long ingressTime;
	private boolean loaded;
	private String uid;
	private boolean markDelete;
	
	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeByteArray(messageKey);
		out.writeByteArray(messageValue);
		out.writeUTF(keyCheksum);
		out.writeUTF(valueCheksum);
		out.writeLong(ingressTime);
		out.writeLong(originTime);
		out.writeBoolean(loaded);
		out.writeUTF(uid);
		out.writeBoolean(markDelete);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		setMessageKey(in.readByteArray());
		setMessageValue(in.readByteArray());
		setKeyCheksum(in.readUTF());
		setValueCheksum(in.readUTF());
		setIngressTime(in.readLong());
		setOriginTime(in.readLong());
		setLoaded(in.readBoolean());
		setUid(in.readUTF());
		setMarkDelete(in.readBoolean());
	}

	public int size() {
		return (messageKey != null ? messageKey.length : 0) + (messageValue != null ? messageValue.length : 0);
	}

	public boolean isLoaded() {
		return loaded;
	}

	public void setLoaded(boolean loaded) {
		this.loaded = loaded;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public boolean isMarkDelete() {
		return markDelete;
	}

	public void setMarkDelete(boolean markDelete) {
		this.markDelete = markDelete;
	}

}
