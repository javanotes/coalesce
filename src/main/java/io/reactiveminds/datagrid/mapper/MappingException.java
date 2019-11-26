package io.reactiveminds.datagrid.mapper;

public class MappingException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MappingException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public MappingException(String msg) {
		super(msg);
	}
}
