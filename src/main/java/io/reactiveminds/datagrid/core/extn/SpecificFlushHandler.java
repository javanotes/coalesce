package io.reactiveminds.datagrid.core.extn;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.specific.SpecificRecord;

import io.reactiveminds.datagrid.api.FlushHandler;
import io.reactiveminds.datagrid.vo.KeyValRecord;
import io.reactiveminds.datagrid.vo.SpecificKeyValRecord;

public abstract class SpecificFlushHandler<K extends SpecificRecord,V extends SpecificRecord> implements FlushHandler {

	protected abstract int[] doApply(List<SpecificKeyValRecord<K, V>> requests);
	@Override
	public int[] apply(List<KeyValRecord> flushRequests) {
		List<SpecificKeyValRecord<K, V>> specific = flushRequests.stream().map(k -> new SpecificKeyValRecord<K, V>(k)).collect(Collectors.toList());
		return doApply(specific);
	}

}
