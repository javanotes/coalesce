package io.reactiveminds.datagrid.core;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryEvictedListener;

import io.reactiveminds.datagrid.api.FlushHandler;
import io.reactiveminds.datagrid.api.SurvivorRule;
import io.reactiveminds.datagrid.err.ProcessFailedException;
import io.reactiveminds.datagrid.notif.EventType;
import io.reactiveminds.datagrid.spi.GridContext;
import io.reactiveminds.datagrid.spi.EventsNotifier;
import io.reactiveminds.datagrid.util.Utils;
import io.reactiveminds.datagrid.vo.DataEvent;
import io.reactiveminds.datagrid.vo.KeyValRecord;

abstract class AbstractProcessor implements IProcessor, EntryEvictedListener<byte[], DataEvent> {
	private static final Logger log = LoggerFactory.getLogger("AbstractProcessor");
	/**
	 * 
	 * @param keySchema
	 * @param valSchema
	 * @param imap
	 */
	protected AbstractProcessor(Schema keySchema, Schema valSchema, String imap) {
		super();
		this.keySchema = keySchema;
		this.valSchema = valSchema;
		this.imap = imap;
	}
	protected Schema keySchema;
	protected Schema valSchema;
	protected String imap;
	@Autowired
	EventsNotifier notifier;
	protected SurvivorRule rule;
	private FlushHandler flusher;
	
	@Override
	public void setSurvivalRule(SurvivorRule rule) {
		this.rule = rule;
	}

	@Override
	public void setFlushHandler(FlushHandler flush) {
		this.flusher = flush;
	}
	@Autowired
	private HazelcastInstance hz;
	/**
	 * Get the coalesce map
	 * @return
	 */
	IMap<byte[], DataEvent> map(){ return hz.getMap(this.imap);}
	@Autowired
	GridContext gridContext;
	/**
	 * 
	 * @param req
	 */
	protected void applyAndSet(DataEvent req) {
		DataEvent oldVal = map().get(req.getMessageKey());
		if(oldVal != null) {
			try {
				GenericRecord merged = rule.merge(Utils.fromAvroBytes(oldVal.getMessageValue(), valSchema), Utils.fromAvroBytes(req.getMessageValue(), valSchema), gridContext);
				oldVal.setMessageValue(Utils.toAvroBytes(merged));
				oldVal.setValueCheksum(Utils.generateValueChecksum(oldVal.getMessageValue()));
				oldVal.setLoaded(false);
				map().set(req.getMessageKey(), oldVal);
				setDirty(req.getMessageKey());
				
				//return new InputRequest(req.getKey(), oldVal.getValueBytes());
			} 
			catch (Exception e) {
				throw new ProcessFailedException("Exception while applying survival rules", e);
			}
		}
		else {
			//oldVal = new ValueWrapper();
			//oldVal.setValueBytes(req.getValue());
			map().set(req.getMessageKey(), req);
			setDirty(req.getMessageKey());
			
			//return new InputRequest(req.getKey(), oldVal.getValueBytes());
		}
	}
	@PostConstruct
	void init() {
		hz.getMap(this.imap).addLocalEntryListener(this);
	}
	/**
	 * 
	 * @param flushRequests
	 */
	void doFlush(final List<KeyValRecord> flushRequests) {
		//exception handling?
		int[] result = flusher.apply(flushRequests);
		for (int i = 0; i < result.length; i++) {
			if(result[i] >=0 && i < flushRequests.size()) {
				byte[] key = Utils.toAvroBytes(flushRequests.get(i).getKey());
				resetDirty(key);
				log.debug("----- ON_KEY_FLUSH ---- "+flushRequests.get(i).getKey());
				notifier.sendNotification(EventType.FLUSHED_TO_STORE, flushRequests.get(i), Utils.generateKeyChecksum(key));
			}
		}
	}
	private final ConcurrentMap<B, AtomicBoolean> dirtyCache = new ConcurrentHashMap<>();
	void setDirty(byte[] bytes) {
		B b = new B(bytes);
		if(!dirtyCache.containsKey(b)) {
			dirtyCache.putIfAbsent(b, new AtomicBoolean(true));
		}
		AtomicBoolean bool = dirtyCache.get(b);
		if (bool != null) {
			bool.compareAndSet(false, true);
		}
	}
	boolean isDirty(byte[] bytes) {
		B b = new B(bytes);
		AtomicBoolean bool = dirtyCache.get(b);
		if (bool != null) {
			return bool.compareAndSet(true, true);
		}
		return false;
	}
	void resetDirty(byte[] bytes) {
		B b = new B(bytes);
		AtomicBoolean bool = dirtyCache.get(b);
		if (bool != null) {
			bool.compareAndSet(true, false);
		}
	}
	@Override
	public void entryEvicted(EntryEvent<byte[], DataEvent> event) {
		log.warn("Coalesce Entry evicted: "+event.getValue().getKeyCheksum());
		//dirtyCache.remove(new B(event.getKey()));
		process(event.getValue());
	}
	
	protected KeyValRecord newKeyVal(byte[] k, byte[] v) {
		return new KeyValRecord(Utils.fromAvroBytes(k, keySchema),
				Utils.fromAvroBytes(v, valSchema));
	}
	
	protected static class B{
		public B(byte[] b) {
			super();
			this.b = b;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + Arrays.hashCode(b);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			B other = (B) obj;
			if (!Arrays.equals(b, other.b))
				return false;
			return true;
		}

		public final byte[] b;
	}
}
