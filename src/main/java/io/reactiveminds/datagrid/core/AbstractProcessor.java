package io.reactiveminds.datagrid.core;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
import io.reactiveminds.datagrid.api.GridContext;
import io.reactiveminds.datagrid.api.SurvivorRule;
import io.reactiveminds.datagrid.err.ProcessFailedException;
import io.reactiveminds.datagrid.notif.EventType;
import io.reactiveminds.datagrid.spi.IProcessor;
import io.reactiveminds.datagrid.spi.EventsNotifier;
import io.reactiveminds.datagrid.util.Utils;
import io.reactiveminds.datagrid.vo.CoalesceEntry;
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
				
			} 
			catch (Exception e) {
				throw new ProcessFailedException("Exception while applying survival rules", e);
			}
		}
		else {
			map().set(req.getMessageKey(), req);
			setDirty(req.getMessageKey());
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
	void doFlush(final List<CoalesceEntry> flushRequests) {
		try 
		{
			final List<KeyValRecord> collected = flushRequests.stream()
			.map(c -> newKeyVal(c.getKey(), c.getValue()))
			.collect(Collectors.toList());
			
			int[] result = flusher.apply(collected);
			
			for (int i = 0; i < result.length; i++) {
				if(i < flushRequests.size()) {
					byte[] key = flushRequests.get(i).getKey();
					if(result[i] >= 0) {
						//resetDirty(key);
						log.debug("----- ON_KEY_FLUSH ---- "+flushRequests.get(i).getKey());
						notifier.sendNotification(EventType.FLUSHED_TO_STORE, collected.get(i), "");//meaningless to get tracing id now. it is coalesced by key
					}
					else {
						setDirty(key);
					}
				}
				
			}
		} catch (Exception e) {
			flushRequests.forEach(c -> setDirty(c.getKey()));
			log.error("Unhandled error on flush. Batch rejected", e);
		}
	}
	private final ConcurrentMap<B, AtomicBoolean> dirtyCache = new ConcurrentHashMap<>();
	/**
	 * Set dirty state
	 * @param bytes
	 */
	boolean setDirty(byte[] bytes) {
		B b = new B(bytes);
		if(!dirtyCache.containsKey(b)) {
			dirtyCache.putIfAbsent(b, new AtomicBoolean(true));
		}
		AtomicBoolean bool = dirtyCache.get(b);
		if (bool != null) {
			return bool.compareAndSet(false, true);
		}
		return false;
	}
	/**
	 * If is dirty
	 * @param bytes
	 * @return
	 */
	boolean isDirty(byte[] bytes) {
		B b = new B(bytes);
		AtomicBoolean bool = dirtyCache.get(b);
		if (bool != null) {
			return bool.compareAndSet(true, true);
		}
		return false;
	}
	/**
	 * clear dirty state
	 * @param bytes
	 */
	boolean resetDirty(byte[] bytes) {
		B b = new B(bytes);
		AtomicBoolean bool = dirtyCache.get(b);
		if (bool != null) {
			return bool.compareAndSet(true, false);
		}
		return false;
	}
	@Override
	public void entryEvicted(EntryEvent<byte[], DataEvent> event) {
		//log.warn("Coalesce Entry evicted: "+event.getValue().getKeyCheksum());
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
