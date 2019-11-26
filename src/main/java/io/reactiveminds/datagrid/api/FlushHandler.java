package io.reactiveminds.datagrid.api;

import java.util.List;

import io.reactiveminds.datagrid.vo.KeyValRecord;

public interface FlushHandler {
	/**
	 * 
	 * @param flushRequests
	 * @return int[] in the lines of {@linkplain Statement#executeBatch()}
	 */
	int[] apply(List<KeyValRecord> flushRequests);

}
