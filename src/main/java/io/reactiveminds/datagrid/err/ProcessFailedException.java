package io.reactiveminds.datagrid.err;

import org.springframework.core.NestedRuntimeException;

public class ProcessFailedException extends NestedRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ProcessFailedException(String msg) {
		super(msg);
	}

	public ProcessFailedException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
