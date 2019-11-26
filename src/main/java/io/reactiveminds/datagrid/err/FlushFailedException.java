package io.reactiveminds.datagrid.err;

import org.springframework.core.NestedRuntimeException;

public class FlushFailedException extends NestedRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public FlushFailedException(String msg) {
		super(msg);
	}

	public FlushFailedException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
