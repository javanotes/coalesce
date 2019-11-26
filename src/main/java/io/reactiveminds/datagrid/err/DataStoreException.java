package io.reactiveminds.datagrid.err;

import org.springframework.dao.DataAccessException;

public class DataStoreException extends DataAccessException {

	public DataStoreException(String msg, Throwable cause) {
		super(msg, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
