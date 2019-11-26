package io.reactiveminds.datagrid.err;

import org.springframework.core.NestedRuntimeException;

public class ConfigurationException extends NestedRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2112487703882842892L;

	public ConfigurationException(String msg) {
		super(msg);
	}

	public ConfigurationException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
