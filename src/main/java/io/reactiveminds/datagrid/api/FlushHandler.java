package io.reactiveminds.datagrid.api;

import java.util.List;

import io.reactiveminds.datagrid.vo.KeyValRecord;

public interface FlushHandler {
	/**
	 * Apply a list of datastore updates (save/delete)
	 * @param flushRequests - if value is null, implies delete
	 * @return int[] in the lines of {@linkplain Statement#executeBatch()}
	 */
	int[] apply(List<KeyValRecord> flushRequests);

}
