package io.reactiveminds.datagrid.core;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactiveminds.datagrid.err.ProcessFailedException;
import io.reactiveminds.datagrid.notif.EventType;
import io.reactiveminds.datagrid.vo.CoalesceEntry;
import io.reactiveminds.datagrid.vo.DataEvent;
import io.reactiveminds.datagrid.vo.KeyValRecord;

class DefaultProcessor extends AbstractProcessor {
	private static final Logger log = LoggerFactory.getLogger("DefaultProcessor");
	/**
	 * 
	 * @param keySchema
	 * @param valSchema
	 * @param imap
	 */
	public DefaultProcessor(Schema keySchema, Schema valSchema, String imap) {
		super(keySchema, valSchema, imap);
	}
	//one processor instance per map
	
	private long time = 1000;
	private TimeUnit timeunit = TimeUnit.MILLISECONDS;
	
	private ReadWriteLock rwLock = new ReentrantReadWriteLock();
	
	@Override
	public void process(DataEvent req) {
		log.debug("new request of size: "+req.size());
		Lock rLock = rwLock.readLock();
		rLock.lock();
		try 
		{
			boolean locked = map().tryLock(req.getMessageKey(), time, timeunit);
			if(locked) {
				try {
					applyAndSet(req);
					notifier.sendNotification(EventType.APPLIED_TO_GRID, req, keySchema, valSchema);
					log.debug("----- ON_APPLY_SET ----");
				}
				finally {
					map().unlock(req.getMessageKey());
				}
			}
			else {
				throw new ProcessFailedException("Unable to acquire key lock");
			}
		} 
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		finally {
			rLock.unlock();
		}
	}

	@Override
	public void flush() {
		Lock wLock = rwLock.writeLock();
		final List<KeyValRecord> flushRequests = new LinkedList<>();
		wLock.lock();
		try {
			
			Stream<CoalesceEntry> stream = map().localKeySet().parallelStream()
			.map(k -> new CoalesceEntry(k, map().get(k).getMessageValue()));
			
			List<KeyValRecord> collected = stream.filter(c -> isDirty(c.getKey()))
			.map(c -> newKeyVal(c.getKey(), c.getValue()))
			.collect(Collectors.toList());
			
			flushRequests.addAll(collected);
			
		}
		finally {
			wLock.unlock();
		}
		//do we stay inside write lock?
		//is there a chance of load in the meantime?
		if (!flushRequests.isEmpty()) {
			doFlush(flushRequests);
		}
	}
}
